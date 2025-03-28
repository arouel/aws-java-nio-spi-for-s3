/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.nio.file.StandardOpenOption.*;
import static org.assertj.core.api.BDDAssertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

@SuppressWarnings("unchecked")
class S3AppendableByteChannelTest {

    @Test
    void test_construct_CompletionException_to_IOException() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutLow()).thenReturn(null);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = new RuntimeException("unknown runtime exception");
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not open the path: somefile");
    }

    @Test
    void test_construct_InterruptedException_to_IOException_withTimeout() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutLow()).thenReturn(1L);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var exception = new InterruptedException("test interruption");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(responseFuture);

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(IOException.class)
            .hasCause(exception)
            .hasMessage("Could not open the path: " + path);
    }

    @Test
    void test_construct_NoSuchKeyException_ignoreWhenCreateNewOptionIsPresent() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = NoSuchKeyException.builder().build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenCode(() -> new S3AppendableByteChannel(path, client, Set.of(CREATE_NEW))).doesNotThrowAnyException();
    }

    @Test
    void test_construct_NoSuchKeyException_ignoreWhenCreateOptionIsPresent() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = NoSuchKeyException.builder().build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenCode(() -> new S3AppendableByteChannel(path, client, Set.of(CREATE))).doesNotThrowAnyException();
    }

    @Test
    void test_construct_NoSuchKeyException_to_NoSuchFileException() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = NoSuchKeyException.builder().build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage(path.toString());
    }

    @Test
    void test_construct_RuntimeException_to_IOException() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = new RuntimeException("unknown runtime exception");
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not open the path: somefile");
    }

    @Test
    void test_construct_S3Exception_to_S3TransferException() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var cause = S3Exception.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("Forbidden")
                .errorMessage("S3 HEAD request failed for '" + path + "'")
                .build())
            .numAttempts(1)
            .requestId("some-request")
            .statusCode(403)
            .build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(S3TransferException.class)
            .hasMessage("HeadObject => 403; somefile; S3 HEAD request failed for 'somefile'");
    }

    @Test
    void test_construct_TimeoutException_to_IOException() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutLow()).thenReturn(1L);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var exception = new TimeoutException("test timeout");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(responseFuture);

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of()))
            .isInstanceOf(IOException.class)
            .hasCause(exception)
            .hasMessage("Could not open the path: " + path);
    }

    @Test
    void test_construct_whenCreateNewOptionIsPresentThrowExceptionIfExists() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var response = HeadObjectResponse.builder().build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        thenThrownBy(() -> new S3AppendableByteChannel(path, client, Set.of(CREATE_NEW)))
            .isInstanceOf(FileAlreadyExistsException.class)
            .hasMessage(path.toString());
    }

    @Test
    void test_construct_whenCreateOptionIsPresentIfExistsAppend() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.getConfiguration()).thenReturn(mock(S3NioSpiConfiguration.class));
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var response = HeadObjectResponse.builder().contentLength(123L).build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        try (var channel = new S3AppendableByteChannel(path, client, Set.of(CREATE))) {
            then(channel.size()).isEqualTo(123);
        }
    }

    @Test
    void test_write_exists() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var headObjectResponse = HeadObjectResponse.builder().contentLength(3L).build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(headObjectResponse));

        var putObjectResponse = PutObjectResponse.builder().size(6L).build();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(putObjectResponse));

        try (var channel = new S3AppendableByteChannel(path, client, Set.of())) {
            then(channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 }))).isEqualTo(3);
            then(channel.size()).isEqualTo(6);
        }
    }

    @Test
    void test_write_failure() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(NoSuchKeyException.builder().build()));

        var cause = S3Exception.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("Forbidden")
                .errorMessage("S3 HEAD request failed for '" + path + "'")
                .build())
            .numAttempts(1)
            .requestId("some-request")
            .statusCode(403)
            .build();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.failedFuture(cause));

        try (var channel = new S3AppendableByteChannel(path, client, Set.of(CREATE))) {
            thenThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 })))
                .isInstanceOf(S3TransferException.class)
                .hasMessage("PutObject => 403; somefile; S3 HEAD request failed for 'somefile'");
        }
    }

    @Test
    void test_write_InterruptedException_to_IOException() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutHigh()).thenReturn(1L);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var headObjectResponse = HeadObjectResponse.builder().contentLength(3L).build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(headObjectResponse));

        var exception = new InterruptedException("test interruption");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(responseFuture);

        try (var channel = new S3AppendableByteChannel(path, client, Set.of())) {
            thenThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 })))
                .isInstanceOf(IOException.class)
                .hasCause(exception)
                .hasMessage("Could not write to path: " + path);
        }
    }

    @Test
    void test_write_notExists() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutHigh()).thenReturn(null);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(NoSuchKeyException.builder().build()));

        var putObjectResponse = PutObjectResponse.builder().build();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(putObjectResponse));

        try (var channel = new S3AppendableByteChannel(path, client, Set.of(CREATE))) {
            then(channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 }))).isEqualTo(3);
            then(channel.size()).isEqualTo(3);
        }
    }

    @Test
    void test_write_TimeoutException_to_IOException() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutHigh()).thenReturn(1L);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var headObjectResponse = HeadObjectResponse.builder().contentLength(3L).build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(headObjectResponse));

        var exception = new TimeoutException("test timeout");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(responseFuture);

        try (var channel = new S3AppendableByteChannel(path, client, Set.of())) {
            thenThrownBy(() -> channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 })))
                .isInstanceOf(IOException.class)
                .hasCause(exception)
                .hasMessage("Could not write to path: " + path);
        }
    }

    @Test
    void test_write_withTimeout() throws IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        var configuration = mock(S3NioSpiConfiguration.class);
        when(configuration.getTimeoutHigh()).thenReturn(1L);
        when(fs.getConfiguration()).thenReturn(configuration);
        when(fs.provider()).thenReturn(provider);
        var path = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var headObjectResponse = HeadObjectResponse.builder().contentLength(3L).build();
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(headObjectResponse));

        var putObjectResponse = PutObjectResponse.builder().size(6L).build();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(putObjectResponse));

        try (var channel = new S3AppendableByteChannel(path, client, Set.of())) {
            then(channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3 }))).isEqualTo(3);
            then(channel.size()).isEqualTo(6);
        }
    }

}
