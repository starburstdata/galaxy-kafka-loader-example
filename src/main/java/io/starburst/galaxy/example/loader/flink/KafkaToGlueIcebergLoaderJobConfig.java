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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaToGlueIcebergLoaderJobConfig
{
    private final Region s3Region;
    private final String s3Bucket;
    private final Region glueRegion;
    private final String glueDatabase;
    private final String glueTable;
    private final Region secretsManagerRegion;
    private final String kafkaCredentialsSecretName;
    private final String kafkaCredentialsApiKeyField;
    private final String kafkaCredentialsApiSecretField;
    private final List<String> kafkaBootstrapServers;
    private final Duration kafkaPartitionDiscoveryInterval;
    private final List<String> kafkaTopics;
    private final Optional<String> kafkaDeadLetterQueueTopic;
    private final String kafkaClientGroupId;
    private final Duration flushInterval;
    private final String unnestJsonPointer;
    private final Optional<CaseFormat> inputFieldNameCase;
    private final Optional<CaseFormat> outputFieldNameCase;
    private final List<InputFieldProjection> inputFieldProjections;

    public KafkaToGlueIcebergLoaderJobConfig(
            Region s3Region,
            String s3Bucket,
            Region glueRegion,
            String glueDatabase,
            String glueTable,
            Region secretsManagerRegion,
            String kafkaCredentialsSecretName,
            String kafkaCredentialsApiKeyField,
            String kafkaCredentialsApiSecretField,
            List<String> kafkaBootstrapServers,
            Duration kafkaPartitionDiscoveryInterval,
            List<String> kafkaTopics,
            Optional<String> kafkaDeadLetterQueueTopic,
            String kafkaClientGroupId,
            Duration flushInterval,
            String unnestJsonPointer,
            Optional<CaseFormat> inputFieldNameCase,
            Optional<CaseFormat> outputFieldNameCase,
            List<InputFieldProjection> inputFieldProjections)
    {
        this.s3Region = requireNonNull(s3Region, "s3Region is null");
        this.s3Bucket = requireNonNull(s3Bucket, "s3 is null");
        this.glueRegion = requireNonNull(glueRegion, "glueRegion is null");
        this.glueDatabase = requireNonNull(glueDatabase, "glueDatabase is null");
        this.glueTable = requireNonNull(glueTable, "glueTable is null");
        this.secretsManagerRegion = requireNonNull(secretsManagerRegion, "secretsRegion is null");
        this.kafkaCredentialsSecretName = requireNonNull(kafkaCredentialsSecretName, "kafkaCredentialsSecretName is null");
        this.kafkaCredentialsApiKeyField = requireNonNull(kafkaCredentialsApiKeyField, "kafkaCredentialsApiKeyField is null");
        this.kafkaCredentialsApiSecretField = requireNonNull(kafkaCredentialsApiSecretField, "kafkaCredentialsApiSecretField is null");
        this.kafkaBootstrapServers = ImmutableList.copyOf(requireNonNull(kafkaBootstrapServers, "kafkaBootstrapServers is null"));
        this.kafkaPartitionDiscoveryInterval = requireNonNull(kafkaPartitionDiscoveryInterval, "kafkaPartitionDiscoveryInterval is null");
        this.kafkaTopics = ImmutableList.copyOf(requireNonNull(kafkaTopics, "kafkaTopic is null"));
        this.kafkaDeadLetterQueueTopic = requireNonNull(kafkaDeadLetterQueueTopic, "kafkaDeadLetterQueueTopic is null");
        this.kafkaClientGroupId = requireNonNull(kafkaClientGroupId, "kafkaClientGroupId is null");
        this.flushInterval = requireNonNull(flushInterval, "flushInterval is null");
        this.unnestJsonPointer = requireNonNull(unnestJsonPointer, "unnestJsonPointer is null");
        this.inputFieldNameCase = requireNonNull(inputFieldNameCase, "inputFieldNameCase is null");
        this.outputFieldNameCase = requireNonNull(outputFieldNameCase, "outputFieldNameCase is null");
        this.inputFieldProjections = ImmutableList.copyOf(requireNonNull(inputFieldProjections, "inputFieldProjections is null"));
    }

    public Region getS3Region()
    {
        return s3Region;
    }

    public String getS3Bucket()
    {
        return s3Bucket;
    }

    public Region getGlueRegion()
    {
        return glueRegion;
    }

    public String getGlueDatabase()
    {
        return glueDatabase;
    }

    public String getGlueTable()
    {
        return glueTable;
    }

    public Region getSecretsManagerRegion()
    {
        return secretsManagerRegion;
    }

    public String getKafkaCredentialsSecretName()
    {
        return kafkaCredentialsSecretName;
    }

    public String getKafkaCredentialsApiKeyField()
    {
        return kafkaCredentialsApiKeyField;
    }

    public String getKafkaCredentialsApiSecretField()
    {
        return kafkaCredentialsApiSecretField;
    }

    public List<String> getKafkaBootstrapServers()
    {
        return kafkaBootstrapServers;
    }

    public Duration getKafkaPartitionDiscoveryInterval()
    {
        return kafkaPartitionDiscoveryInterval;
    }

    public List<String> getKafkaTopics()
    {
        return kafkaTopics;
    }

    public Optional<String> getKafkaDeadLetterQueueTopic()
    {
        return kafkaDeadLetterQueueTopic;
    }

    public String getKafkaClientGroupId()
    {
        return kafkaClientGroupId;
    }

    public Duration getFlushInterval()
    {
        return flushInterval;
    }

    public String getUnnestJsonPointer()
    {
        return unnestJsonPointer;
    }

    public Optional<CaseFormat> getInputFieldNameCase()
    {
        return inputFieldNameCase;
    }

    public Optional<CaseFormat> getOutputFieldNameCase()
    {
        return outputFieldNameCase;
    }

    public List<InputFieldProjection> getInputFieldProjections()
    {
        return inputFieldProjections;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Region s3Region;
        private String s3Bucket;
        private Region glueRegion;
        private String glueDatabase;
        private String glueTable;
        private Region secretsManagerRegion;
        private String kafkaCredentialsSecretName;
        private String kafkaCredentialsApiKeyField = "apiKey";
        private String kafkaCredentialsApiSecretField = "apiSecret";
        private List<String> kafkaBootstrapServers;
        private Duration kafkaPartitionDiscoveryInterval = new Duration(30, TimeUnit.SECONDS);
        private List<String> kafkaTopics;
        private Optional<String> kafkaDeadLetterQueueTopic;
        private String kafkaClientGroupId;
        private Duration flushInterval = new Duration(10, TimeUnit.SECONDS);
        private String unnestJsonPointer = "";
        private Optional<CaseFormat> inputFieldNameCase = Optional.empty();
        private Optional<CaseFormat> outputFieldNameCase = Optional.empty();
        private List<InputFieldProjection> inputFieldProjections = ImmutableList.of();

        private Builder() {}

        public Builder setS3Region(Region s3Region)
        {
            this.s3Region = s3Region;
            return this;
        }

        public Builder setS3Bucket(String s3Bucket)
        {
            this.s3Bucket = s3Bucket;
            return this;
        }

        public Builder setGlueRegion(Region glueRegion)
        {
            this.glueRegion = glueRegion;
            return this;
        }

        public Builder setGlueDatabase(String glueDatabase)
        {
            this.glueDatabase = glueDatabase;
            return this;
        }

        public Builder setGlueTable(String glueTable)
        {
            this.glueTable = glueTable;
            return this;
        }

        public Builder setSecretsManagerRegion(Region secretsManagerRegion)
        {
            this.secretsManagerRegion = secretsManagerRegion;
            return this;
        }

        public Builder setKafkaCredentialsSecretName(String kafkaCredentialsSecretName)
        {
            this.kafkaCredentialsSecretName = kafkaCredentialsSecretName;
            return this;
        }

        public Builder setKafkaCredentialsApiKeyField(String kafkaCredentialsApiKeyField)
        {
            this.kafkaCredentialsApiKeyField = kafkaCredentialsApiKeyField;
            return this;
        }

        public Builder setKafkaCredentialsApiSecretField(String kafkaCredentialsApiSecretField)
        {
            this.kafkaCredentialsApiSecretField = kafkaCredentialsApiSecretField;
            return this;
        }

        public Builder setKafkaBootstrapServers(List<String> kafkaBootstrapServers)
        {
            this.kafkaBootstrapServers = ImmutableList.copyOf(kafkaBootstrapServers);
            return this;
        }

        public Builder setKafkaPartitionDiscoveryInterval(Duration kafkaPartitionDiscoveryInterval)
        {
            this.kafkaPartitionDiscoveryInterval = kafkaPartitionDiscoveryInterval;
            return this;
        }

        public Builder setKafkaTopics(List<String> kafkaTopics)
        {
            this.kafkaTopics = ImmutableList.copyOf(kafkaTopics);
            return this;
        }

        public Builder setKafkaDeadLetterQueueTopic(Optional<String> kafkaDeadLetterQueueTopic)
        {
            this.kafkaDeadLetterQueueTopic = kafkaDeadLetterQueueTopic;
            return this;
        }

        public Builder setKafkaClientGroupId(String kafkaClientGroupId)
        {
            this.kafkaClientGroupId = kafkaClientGroupId;
            return this;
        }

        public Builder setFlushInterval(Duration flushInterval)
        {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder setUnnestJsonPointer(String unnestJsonPointer)
        {
            this.unnestJsonPointer = unnestJsonPointer;
            return this;
        }

        public Builder setInputFieldNameCase(Optional<CaseFormat> inputFieldNameCase)
        {
            this.inputFieldNameCase = inputFieldNameCase;
            return this;
        }

        public Builder setOutputFieldNameCase(Optional<CaseFormat> outputFieldNameCase)
        {
            this.outputFieldNameCase = outputFieldNameCase;
            return this;
        }

        /**
         * Field projection JSON pointers are relative to the original JSON object (not the unnested version).
         * Field projection field names will not pass through any case conversions.
         */
        public Builder setInputFieldProjections(List<InputFieldProjection> inputFieldProjections)
        {
            this.inputFieldProjections = ImmutableList.copyOf(inputFieldProjections);
            return this;
        }

        public KafkaToGlueIcebergLoaderJobConfig build()
        {
            return new KafkaToGlueIcebergLoaderJobConfig(
                    s3Region,
                    s3Bucket,
                    glueRegion,
                    glueDatabase,
                    glueTable,
                    secretsManagerRegion,
                    kafkaCredentialsSecretName,
                    kafkaCredentialsApiKeyField,
                    kafkaCredentialsApiSecretField,
                    kafkaBootstrapServers,
                    kafkaPartitionDiscoveryInterval,
                    kafkaTopics,
                    kafkaDeadLetterQueueTopic,
                    kafkaClientGroupId,
                    flushInterval,
                    unnestJsonPointer,
                    inputFieldNameCase,
                    outputFieldNameCase,
                    inputFieldProjections);
        }
    }
}
