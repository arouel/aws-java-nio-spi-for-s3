/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3AppendableByteChannel implements WritableByteChannel {
    private final S3AsyncClient client;
    private final S3Path path;

    private boolean open;
    private long writeOffsetBytes;

    S3AppendableByteChannel(
        S3Path path,
        S3AsyncClient client,
        Set<? extends OpenOption> options)
            throws IOException {
        this.path = Objects.requireNonNull(path);
        this.client = Objects.requireNonNull(client);
        Objects.requireNonNull(options);

        var createIsSet = options.contains(StandardOpenOption.CREATE);
        var createNewIsSet = options.contains(StandardOpenOption.CREATE_NEW);
        try {
            var request = HeadObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey())
                .build();
            var configuration = path.getFileSystem().getConfiguration();
            var future = client.headObject(request);
            var timeout = configuration.getTimeoutLow();
            var response = timeout == null
                ? future.join()
                : future.get(timeout, MINUTES);
            if (createNewIsSet) {
                throw new FileAlreadyExistsException(path.toString());
            }
            writeOffsetBytes = response.contentLength();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not open the path: " + path, e);
        } catch (TimeoutException e) {
            throw new IOException("Could not open the path: " + path, e);
        } catch (CompletionException | ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof NoSuchKeyException) {
                if (!createIsSet && !createNewIsSet) {
                    throw new NoSuchFileException(path.toString());
                }
                writeOffsetBytes = 0;
                return;
            }
            if (!(cause instanceof AwsServiceException)) {
                throw new IOException("Could not open the path: " + path, e);
            }
            var s3e = (AwsServiceException) cause;
            throw new S3TransferException("HeadObject", path, s3e);
        }
        open = true;
    }

    @Override
    public void close() throws IOException {
        if (!open) {
            // channel has already been closed -> close() should have no effect
            return;
        }
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    long size() {
        return writeOffsetBytes;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int bytesToAppend = src.remaining();
        var request = PutObjectRequest.builder()
            .bucket(path.bucketName())
            .key(path.getKey())
            .writeOffsetBytes(writeOffsetBytes)
            .build();
        var requestBody = AsyncRequestBody.fromByteBuffer(src);
        var configuration = path.getFileSystem().getConfiguration();
        try {
            var future = client.putObject(request, requestBody);
            var timeout = configuration.getTimeoutHigh();
            var response = timeout == null
                ? future.join()
                : future.get(timeout, MINUTES);
            writeOffsetBytes = response.size() == null
                ? bytesToAppend
                : response.size().intValue();
            return bytesToAppend;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not write to path: " + path, e);
        } catch (TimeoutException e) {
            throw new IOException("Could not write to path: " + path, e);
        } catch (CompletionException | ExecutionException e) {
            var cause = e.getCause();
            if (!(cause instanceof AwsServiceException)) {
                throw new IOException("Could not write to path: " + path, e);
            }
            var s3e = (AwsServiceException) cause;
            throw new S3TransferException("PutObject", path, s3e);
        }
    }
}
