/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.*;
import static java.nio.file.StandardOpenOption.*;
import static org.assertj.core.api.Assertions.*;
import static software.amazon.nio.spi.s3.Containers.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("Files$newByteChannel* should read and write on S3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesNewByteChannelTest {

    String bucketName;

    @BeforeEach
    public void createBucket() {
        bucketName = "byte-channel-bucket" + System.currentTimeMillis();
        Containers.createBucket(bucketName);
        // reset time to scope the log entries for each test case
        assertThat(Containers.getLoggedS3HttpRequests()).contains("CreateBucket => 200");
    }

    @Test
    @DisplayName("newByteChannel with CREATE_NEW, WRITE, APPEND throws S3TransferException on failing write")
    public void newByteChannel_APPEND___CREATE_NEW_WRITE_fails() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-append-cnw-test.txt"));
        try (var channel = Files.newByteChannel(path, CREATE_NEW, WRITE, APPEND)) {
            Containers.deleteBucket(bucketName);

            assertThatThrownBy(() -> channel.write(ByteBuffer.wrap("abc".getBytes())))
                .isInstanceOf(S3TransferException.class)
                .hasMessage("PutObject => 404; /bc-append-cnw-test.txt; The specified bucket does not exist");
        }
    }

    @Test
    @DisplayName("newByteChannel with CREATE_NEW, WRITE, APPEND creates a new S3 object if not exists")
    public void newByteChannel_APPEND___CREATE_NEW_WRITE_succeeds() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-append-cnw-test.txt"));
        try (var channel = Files.newByteChannel(path, CREATE_NEW, WRITE, APPEND)) {
            channel.write(ByteBuffer.wrap("abc".getBytes()));
        }
        assertThat(path).hasContent("abc");
    }

    @Test
    @DisplayName("newByteChannel with CREATE_NEW, WRITE, APPEND throws an exception when the object exists")
    public void newByteChannel_APPEND___CREATE_NEW_WRITE_throwsWhenExists() throws IOException {
        var path = putObject(bucketName, "bc-append-cnw-test.txt", "abc");

        assertThatThrownBy(() -> Files.newByteChannel(path, CREATE_NEW, WRITE, APPEND))
            .isInstanceOf(FileAlreadyExistsException.class)
            .hasMessage(path.toString());
    }

    @Test
    @DisplayName("newByteChannel with CREATE, WRITE, APPEND appends to an empty S3 object")
    public void newByteChannel_APPEND___CREATE_WRITE_existsAndEmpty() throws IOException {
        var path = putObject(bucketName, "bc-append-cw-test.txt");

        try (var channel = Files.newByteChannel(path, CREATE, WRITE, APPEND)) {
            // LocalStack 4.2 does not support PutObject with 'x-amz-write-offset-bytes' header
            assertThat(channel.write(ByteBuffer.wrap("def".getBytes()))).isEqualTo(3);
        }

        assertThat(path).hasContent("def");
        // LocalStack 4.2 does not support PutObject with 'x-amz-write-offset-bytes' header
        // assertThat(path).hasContent("abcdef");
    }

    @Test
    @DisplayName("newByteChannel with CREATE, WRITE, APPEND creates a new S3 object if not exists")
    public void newByteChannel_APPEND___CREATE_WRITE_notExists() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-cwa-test.txt"));

        try (var channel = Files.newByteChannel(path, CREATE, WRITE, APPEND)) {
            assertThat(channel.write(ByteBuffer.wrap("abc".getBytes()))).isEqualTo(3);
            // LocalStack 4.2 does not support PutObject with 'x-amz-write-offset-bytes' header
            // assertThat(channel.write(ByteBuffer.wrap("def".getBytes()))).isEqualTo(6);
        }

        assertThat(path).hasContent("abc");
        // LocalStack 4.2 does not support PutObject with 'x-amz-write-offset-bytes' header
        // assertThat(path).hasContent("abcdef");
    }

    @Test
    @DisplayName("newByteChannel with WRITE, APPEND succeeds when the S3 object exists")
    public void newByteChannel_APPEND___WRITE_succeeds() throws IOException {
        var path = putObject(bucketName, "bc-append-w-test.txt", "abc");

        assertThatCode(() -> Files.newByteChannel(path, WRITE, APPEND)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("newByteChannel with WRITE, APPEND throws when the S3 object not exists")
    public void newByteChannel_APPEND___WRITE_throws() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-append-w-test.txt"));

        assertThatThrownBy(() -> Files.newByteChannel(path, WRITE, APPEND))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage(path.toString());
    }

    @Test
    @DisplayName("newByteChannel with CREATE and WRITE is supported")
    public void newByteChannel_CREATE_WRITE() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-create-write-test.txt"));

        String text = "we test Files#newByteChannel";
        try (var channel = Files.newByteChannel(path, CREATE, WRITE)) {
            channel.write(ByteBuffer.wrap(text.getBytes()));
        }

        assertThat(path).hasContent(text);
    }

    @Test
    @DisplayName("newByteChannel with If-Match header fails")
    void newByteChannel_preventConcurrentOverwrite_withConcurrency() throws IOException {
        var path = putObject(bucketName, "bc-read-write-test.txt", "abc");

        var channel1 = Files.newByteChannel(path, READ, WRITE, S3OpenOption.preventConcurrentOverwrite());
        channel1.write(ByteBuffer.wrap("def".getBytes()));

        try (var channel2 = Files.newByteChannel(path, READ, WRITE, S3OpenOption.preventConcurrentOverwrite())) {
            channel2.write(ByteBuffer.wrap("ghi".getBytes()));
        }

        assertThatThrownBy(() -> channel1.close())
            .isInstanceOf(IOException.class)
            .hasMessage("PutObject => 412; " + path + "; At least one of the pre-conditions you specified did not hold");

        assertThat(path).hasContent("ghi");
    }

    @Test
    @DisplayName("newByteChannel with If-Match header succeeds")
    void newByteChannel_preventConcurrentOverwrite_withoutConcurrency() throws IOException {
        var path = putObject(bucketName, "bc-read-write-test.txt", "abc");

        var preventConcurrentOverwrite = S3OpenOption.preventConcurrentOverwrite();
        try (var channel = Files.newByteChannel(path, READ, WRITE, preventConcurrentOverwrite)) {

            // write
            channel.position(0);
            channel.write(ByteBuffer.wrap("def".getBytes()));
        }

        assertThat(path).hasContent("def");
    }

    @Test
    @DisplayName("newByteChannel with `PutOnlyIfModified` option and no write operation is performed")
    public void newByteChannel_putOnlyIfModified_noWrite() throws IOException {
        String content = "abc";
        var path = putObject(bucketName, "bc-putOnlyIfModified-test.txt", content);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        try (var channel = Files.newByteChannel(path, READ, WRITE, S3OpenOption.putOnlyIfModified())) {
            assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("HeadObject => 200", "GetObject => 206");

            channel.write(ByteBuffer.wrap(content.getBytes()));
        }
        assertThat(Containers.getLoggedS3HttpRequests()).isEmpty();
    }

    @Test
    @DisplayName("newByteChannel with `PutOnlyIfModified` option and file contents have changed")
    public void newByteChannel_putOnlyIfModified_writeWithChange() throws IOException {
        String content = "abc";
        var path = putObject(bucketName, "bc-putOnlyIfModified-test.txt", content);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        try (var channel = Files.newByteChannel(path, READ, WRITE, S3OpenOption.putOnlyIfModified())) {
            assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("HeadObject => 200", "GetObject => 206");

            channel.write(ByteBuffer.wrap("def".getBytes()));
        }
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");
    }

    @Test
    @DisplayName("newByteChannel with `PutOnlyIfModified` option and no file contents have changed")
    public void newByteChannel_putOnlyIfModified_writeWithoutChange() throws IOException {
        String content = "abc";
        var path = putObject(bucketName, "bc-putOnlyIfModified-test.txt", content);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        try (var channel = Files.newByteChannel(path, READ, WRITE, S3OpenOption.putOnlyIfModified())) {
            assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("HeadObject => 200", "GetObject => 206");

            channel.write(ByteBuffer.wrap(content.getBytes()));
        }
        assertThat(Containers.getLoggedS3HttpRequests()).isEmpty();
    }

    @Test
    @DisplayName("newByteChannel with READ and WRITE is supported")
    public void newByteChannel_READ_WRITE() throws IOException {
        var path = putObject(bucketName, "bc-read-write-test.txt", "xyz");

        String text = "abcdefhij";
        try (var channel = Files.newByteChannel(path, READ, WRITE)) {

            // write
            channel.position(3);
            channel.write(ByteBuffer.wrap("def".getBytes()));
            channel.position(0);
            channel.write(ByteBuffer.wrap("abc".getBytes()));
            channel.position(6);
            channel.write(ByteBuffer.wrap("hij".getBytes()));

            // read
            var dst = ByteBuffer.allocate(text.getBytes().length);
            channel.position(0);
            channel.read(dst);

            // verify
            assertThat(dst.array()).isEqualTo(text.getBytes());
        }

        assertThat(path).hasContent(text);
    }

    @Test
    @DisplayName("newByteChannel with RANGE header to avoid HEAD request")
    public void newByteChannel_useRangeHeader_avoidHeadRequest() throws IOException {
        String content = "xyz";
        var path = putObject(bucketName, "bc-range-test.txt", content);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        var channel = Files.newByteChannel(path, READ, WRITE);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("HeadObject => 200", "GetObject => 206");
        channel.close();
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        var channelWithRangeOption = Files.newByteChannel(path, READ, WRITE, S3OpenOption.range(content.length()));
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("GetObject => 206");
        channelWithRangeOption.close();
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");
    }

    @Test
    @DisplayName("newByteChannel with RANGE header to partially fetch object")
    public void newByteChannel_useRangeHeader_partiallyGet() throws IOException {
        String content = "abcdefghi";
        var path = putObject(bucketName, "bc-range-test.txt", content);
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");

        var channel = Files.newByteChannel(path, READ, WRITE, S3OpenOption.range(3, 6));
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("GetObject => 206");
        ByteBuffer buffer = ByteBuffer.allocate(3);
        channel.read(buffer);
        assertThat(buffer.array()).isEqualTo(new byte[] { 'd', 'e', 'f' });

        channel.close();
        assertThat(Containers.getLoggedS3HttpRequests()).containsExactly("PutObject => 200");
    }

    @Test
    @DisplayName("newByteChannel with CRC32 integrity check")
    public void newByteChannel_withIntegrityCheck_CRC32() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC32").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, CREATE, WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with CRC32C integrity check")
    public void newByteChannel_withIntegrityCheck_CRC32C() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC32C").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, CREATE, WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with CRC64NVME integrity check")
    public void newByteChannel_withIntegrityCheck_CRC64NVME() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC64NVME").execute(() -> {
            var path = (S3Path) Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, CREATE, WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with invalid integrity check")
    public void newByteChannel_withIntegrityCheck_invalid() throws Exception {
        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "invalid").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/int-check-algo-test.txt"));
            assertThatThrownBy(() -> Files.newByteChannel(path)).hasMessage("unknown integrity check algorithm 'invalid'");
        });
    }

}
