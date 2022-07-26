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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import software.amazon.awssdk.regions.Region;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SampleKafkaPublisher
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private SampleKafkaPublisher() {}

    public static void main(String[] args)
            throws JsonProcessingException
    {
        // Galaxy CREATE TABLE statement to match sample schema:
        /*
          CREATE TABLE test_table (
             bool BOOLEAN,
             floating DOUBLE,
             integer BIGINT,
             string VARCHAR,
             bools ARRAY(BOOLEAN),
             floatings ARRAY(DOUBLE),
             integers ARRAY(BIGINT),
             strings ARRAY(VARCHAR),
             string_map MAP(VARCHAR, VARCHAR),
             nested ROW(key VARCHAR, value BIGINT),
             timestamp TIMESTAMP(6) WITH TIME ZONE)
          WITH (
             format = 'PARQUET',
             partitioning = ARRAY['day(timestamp)']);
         */

        // Local mode uses the AWS default credential provider chain in the local environment to access AWS resources

        Region secretsManagerRegion = Region.of(System.getProperty("aws.secret.region")); // AWS SecretsManager region hosting Kafka credentials
        String secretName = System.getProperty("aws.secret.kafkaCredentials.name");
        String apiKeyFieldName = "apiKey"; // JSON field name within secret containing the Kafka API access key
        String apiSecretFieldName = "apiSecret"; // JSON field name with secret containing the Kafka API access secret
        List<String> kafkaServers = Splitter.on(',').splitToList(System.getProperty("kafka.bootstrap.servers"));
        String kafkaTopic = System.getProperty("kafka.topic");

        KafkaCredentials kafkaCredentials = KafkaCredentials.readFromAwsSecretsManager(secretsManagerRegion, secretName, apiKeyFieldName, apiSecretFieldName);
        Properties connectionProperties = KafkaConnections.createConnectionProperties(kafkaCredentials, kafkaServers);
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            SampleRecord sampleRecord = new SampleRecord(
                    true,
                    0.01,
                    1L,
                    "value",
                    ImmutableList.of(true, false, true),
                    ImmutableList.of(0.01, 0.02, 0.03),
                    ImmutableList.of(1L, 2L, 3L),
                    ImmutableList.of("a", "b", "c"),
                    ImmutableMap.of(
                            "k1", "v1",
                            "k2", "v2"),
                    new Nested("key", 1L),
                    OffsetDateTime.now());
            System.out.println("SENDING: " + OBJECT_MAPPER.writeValueAsString(sampleRecord));
            producer.send(new ProducerRecord<>(kafkaTopic, OBJECT_MAPPER.writeValueAsBytes(sampleRecord)));
        }
        System.out.println("===== DONE =====");
    }

    public static class SampleRecord
    {
        private final boolean bool;
        private final double floating;
        private final long integer;
        private final String string;
        private final List<Boolean> bools;
        private final List<Double> floatings;
        private final List<Long> integers;
        private final List<String> strings;
        private final Map<String, String> stringMap;
        private final Nested nested;
        private final OffsetDateTime timestamp;

        @JsonCreator
        public SampleRecord(
                boolean bool,
                double floating,
                long integer,
                String string,
                List<Boolean> bools,
                List<Double> floatings,
                List<Long> integers,
                List<String> strings,
                Map<String, String> stringMap,
                Nested nested,
                OffsetDateTime timestamp)
        {
            this.bool = bool;
            this.floating = floating;
            this.integer = integer;
            this.string = string;
            this.bools = bools;
            this.floatings = floatings;
            this.integers = integers;
            this.strings = strings;
            this.stringMap = stringMap;
            this.nested = nested;
            this.timestamp = timestamp;
        }

        @JsonProperty
        public boolean isBool()
        {
            return bool;
        }

        @JsonProperty
        public double getFloating()
        {
            return floating;
        }

        @JsonProperty
        public long getInteger()
        {
            return integer;
        }

        @JsonProperty
        public String getString()
        {
            return string;
        }

        @JsonProperty
        public List<Boolean> getBools()
        {
            return bools;
        }

        @JsonProperty
        public List<Double> getFloatings()
        {
            return floatings;
        }

        @JsonProperty
        public List<Long> getIntegers()
        {
            return integers;
        }

        @JsonProperty
        public List<String> getStrings()
        {
            return strings;
        }

        @JsonProperty
        public Map<String, String> getStringMap()
        {
            return stringMap;
        }

        @JsonProperty
        public Nested getNested()
        {
            return nested;
        }

        @JsonProperty
        public OffsetDateTime getTimestamp()
        {
            return timestamp;
        }
    }

    public static class Nested
    {
        private final String key;
        private final long value;

        @JsonCreator
        public Nested(String key, long value)
        {
            this.key = key;
            this.value = value;
        }

        @JsonProperty
        public String getKey()
        {
            return key;
        }

        @JsonProperty
        public long getValue()
        {
            return value;
        }
    }
}
