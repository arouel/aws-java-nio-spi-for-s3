/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_PROPERTY;

abstract class Containers {

    static final LocalStackContainer LOCAL_STACK_CONTAINER;

    static {
        LOCAL_STACK_CONTAINER = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.2")
        ).withServices(S3);
        LOCAL_STACK_CONTAINER.start();
        System.setProperty(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, "http");
    }

    static Instant lastLogEntryTime = Instant.now();
    static List<String> getLoggedS3HttpRequests() {
        var logs = Containers.LOCAL_STACK_CONTAINER.getLogs();
        var pattern = Pattern.compile("(?<time>[^ ]+) .*AWS s3.(?<operationName>\\w+) => (?<responseStatus>\\d+)");
        var times = new ArrayList<Instant>();
        times.add(lastLogEntryTime);
        var filteredLogEntries = Arrays.stream(logs.split("\n"))
            .flatMap(line -> {
                var m = pattern.matcher(line);
                if (!m.find()) {
                    return Stream.empty();
                }
                var time = Instant.parse(m.group("time") + "Z");
                times.add(time);
                if (!lastLogEntryTime.isBefore(time)) {
                    return Stream.empty();
                }
                String operationName = m.group("operationName");
                String responseStatus = m.group("responseStatus");
                return Stream.of(operationName + " => " + responseStatus);
            })
            .collect(Collectors.toList());
        lastLogEntryTime = times.get(times.size() - 1);
        return filteredLogEntries;
    }

    public static void createBucket(String name) {
        assertThatCode(() -> {
            var execResult = LOCAL_STACK_CONTAINER.execInContainer(("awslocal s3api create-bucket --bucket " + name).split(" "));
            assertThat(execResult.getExitCode()).isZero();
        }).as("Failed to create bucket '%s'", name)
         .doesNotThrowAnyException();
    }

    public static Path putObject(String bucket, String key) {
        assertThatCode(() -> {
            var execResult = LOCAL_STACK_CONTAINER.execInContainer(("awslocal s3api put-object --bucket " + bucket + " --key " + key).split(" "));
            assertThat(execResult.getExitCode()).isZero();
        }).as("Failed to put object '%s' in bucket '%s'", key, bucket)
         .doesNotThrowAnyException();
        return Paths.get(URI.create(String.format("%s/%s/%s", localStackConnectionEndpoint(), bucket, key)));
    }

    public static Path putObject(String bucket, String key, String content) {
        assertThatCode(() -> {
            var execResultCreateFile = LOCAL_STACK_CONTAINER.execInContainer("sh", "-c", "echo -n '" + content + "' > " + key);
            var execResultPut = LOCAL_STACK_CONTAINER.execInContainer(("awslocal s3api put-object --bucket " + bucket + " --key " + key + " --body " + key).split(" "));

            assertThat(execResultCreateFile.getExitCode()).isZero();
            assertThat(execResultPut.getExitCode()).withFailMessage("Failed put: %s ", execResultPut.getStderr()).isZero();
        }).as("Failed to put object '%s' in bucket '%s'", key, bucket)
                .doesNotThrowAnyException();
        return Paths.get(URI.create(String.format("%s/%s/%s", localStackConnectionEndpoint(), bucket, key)));
    }

    public static String localStackConnectionEndpoint() {
        return localStackConnectionEndpoint(null, null);
    }

    public static String localStackConnectionEndpoint(String key, String secret) {
        var accessKey = key != null ? key : LOCAL_STACK_CONTAINER.getAccessKey();
        var secretKey = secret != null ? secret : LOCAL_STACK_CONTAINER.getSecretKey();
        return String.format("s3x://%s:%s@%s", accessKey, secretKey, localStackHost());
    }

    private static String localStackHost() {
        return LOCAL_STACK_CONTAINER.getEndpoint().getHost() + ":" + LOCAL_STACK_CONTAINER.getEndpoint().getPort();
    }
}
