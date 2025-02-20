/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.*;
import static software.amazon.nio.spi.s3.Containers.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("FileChannel$open* should read and write on S3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesChannelTest {

    String bucketName;

    @BeforeEach
    public void createBucket() {
        bucketName = "file-channel-bucket" + System.currentTimeMillis();
        Containers.createBucket(bucketName);
    }

    @Test
    @DisplayName("open with READ and WRITE is supported")
    public void FileChannel_open() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/file-channel-test.txt"));

        String string = "we test FileChannel#open";
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // write
            channel.write(ByteBuffer.wrap(string.getBytes()), 0);

            // read
            var dst = ByteBuffer.allocate(string.getBytes().length);
            channel.read(dst, 0);
            dst.flip();

            // verify
            assertThat(dst.array()).isEqualTo(string.getBytes());
        }
    }

}
