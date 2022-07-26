/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.starburst.galaxy.example.loader.flink;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.awssdk.regions.Region;

import java.util.Optional;

public final class LocalKafkaToGlueIcebergLoader
{
    private LocalKafkaToGlueIcebergLoader() {}

    public static void main(String[] args)
            throws Exception
    {
        // Local mode uses the AWS default credential provider chain in the local environment to access AWS resources

        KafkaToGlueIcebergLoaderJobConfig config = KafkaToGlueIcebergLoaderJobConfig.builder()
                .setS3Region(Region.of(System.getProperty("aws.s3.region"))) // AWS S3 region hosting data lake data
                .setS3Bucket(System.getProperty("aws.s3.bucket"))
                .setGlueRegion(Region.of(System.getProperty("aws.glue.region"))) // AWS Glue region hosting data lake schema
                .setGlueDatabase(System.getProperty("aws.glue.database"))
                .setGlueTable(System.getProperty("aws.glue.table"))
                .setSecretsManagerRegion(Region.of(System.getProperty("aws.secret.region"))) // AWS SecretsManager region hosting Kafka credentials
                .setKafkaCredentialsSecretName(System.getProperty("aws.secret.kafkaCredentials.name"))
                .setKafkaBootstrapServers(Splitter.on(',').splitToList(System.getProperty("kafka.bootstrap.servers")))
                .setKafkaTopics(ImmutableList.of(System.getProperty("kafka.topic")))
                .setKafkaDeadLetterQueueTopic(Optional.ofNullable(System.getProperty("kafka.dlq.topic")))
                .setKafkaClientGroupId("galaxy_example_loader")
                .build();

        // Local environment checkpoints don't work across restarts since the JobManager also gets restarted:
        // https://lists.apache.org/thread/tgds5wslo48xj6hvkjpnhmvbbdqgnrc7
        KafkaToGlueIcebergLoaderJob.run(StreamExecutionEnvironment.createLocalEnvironment(), config);
    }
}
