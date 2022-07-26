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

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.units.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.ExponentialDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.JsonToRowDataConverters.JsonToRowDataConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.flink.connector.kafka.source.KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS;

public final class KafkaToGlueIcebergLoaderJob
{
    private static final int REQUIRED_JAVA_VERSION = 11;

    private KafkaToGlueIcebergLoaderJob() {}

    public static void main(String[] args)
            throws Exception
    {
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        KafkaToGlueIcebergLoaderJobConfig.Builder configBuilder = KafkaToGlueIcebergLoaderJobConfig.builder();

        String s3PropertyGroupId = "S3Properties";
        configBuilder.setS3Region(Region.of(extractRequiredProperty(applicationProperties, s3PropertyGroupId, "region")))
                .setS3Bucket(extractRequiredProperty(applicationProperties, s3PropertyGroupId, "bucket"));

        String gluePropertyGroupId = "GlueProperties";
        configBuilder.setGlueRegion(Region.of(extractRequiredProperty(applicationProperties, gluePropertyGroupId, "region")))
                .setGlueDatabase(extractRequiredProperty(applicationProperties, gluePropertyGroupId, "database"))
                .setGlueTable(extractRequiredProperty(applicationProperties, gluePropertyGroupId, "table"));

        String secretsManagerPropertyGroupId = "SecretsManagerProperties";
        configBuilder.setSecretsManagerRegion(Region.of(extractRequiredProperty(applicationProperties, secretsManagerPropertyGroupId, "region")));

        String kafkaPropertyGroupId = "KafkaProperties";
        configBuilder.setKafkaCredentialsSecretName(extractRequiredProperty(applicationProperties, kafkaPropertyGroupId, "aws.secret.name"))
                .setKafkaBootstrapServers(Splitter.on(',').splitToList(extractRequiredProperty(applicationProperties, kafkaPropertyGroupId, "bootstrap.servers")))
                .setKafkaTopics(Splitter.on(',').splitToList(extractRequiredProperty(applicationProperties, kafkaPropertyGroupId, "topics")))
                .setKafkaDeadLetterQueueTopic(extractOptionalProperty(applicationProperties, kafkaPropertyGroupId, "dlq-topic"))
                .setKafkaClientGroupId(extractRequiredProperty(applicationProperties, kafkaPropertyGroupId, "client.group.id"));
        extractOptionalProperty(applicationProperties, kafkaPropertyGroupId, "aws.secret.api-key.field")
                .ifPresent(configBuilder::setKafkaCredentialsApiKeyField);
        extractOptionalProperty(applicationProperties, kafkaPropertyGroupId, "aws.secret.api-secret.field")
                .ifPresent(configBuilder::setKafkaCredentialsApiSecretField);
        extractOptionalProperty(applicationProperties, kafkaPropertyGroupId, "partition.discovery.interval")
                .map(Duration::valueOf)
                .ifPresent(configBuilder::setKafkaPartitionDiscoveryInterval);

        String jobPropertyGroupId = "JobProperties";
        extractOptionalProperty(applicationProperties, jobPropertyGroupId, "flush-interval")
                .map(Duration::valueOf)
                .ifPresent(configBuilder::setFlushInterval);
        extractOptionalProperty(applicationProperties, jobPropertyGroupId, "unnest-json-pointer")
                .ifPresent(configBuilder::setUnnestJsonPointer);
        configBuilder.setInputFieldNameCase(extractOptionalProperty(applicationProperties, jobPropertyGroupId, "input.field-name.case-format")
                .map(CaseFormat::valueOf));
        configBuilder.setOutputFieldNameCase(extractOptionalProperty(applicationProperties, jobPropertyGroupId, "output.field-name.case-format")
                .map(CaseFormat::valueOf));

        // Input field projection JSON pointers are relative to the original JSON object (before unnesting)
        // NOTE: we use a group ID prefix since AWS Kinesis Data Analytics has a max of 50 properties per group
        String inputFieldProjectionsGroupIdPrefix = "InputFieldProjections";
        configBuilder.setInputFieldProjections(
                applicationProperties.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(inputFieldProjectionsGroupIdPrefix))
                        .map(Map.Entry::getValue)
                        .map(Maps::fromProperties)
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .map(entry -> new InputFieldProjection(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()));

        run(StreamExecutionEnvironment.getExecutionEnvironment(), configBuilder.build());
    }

    private static String extractRequiredProperty(Map<String, Properties> applicationProperties, String propertyGroupId, String propertyKey)
    {
        Properties properties = applicationProperties.get(propertyGroupId);
        checkArgument(properties != null, "Runtime configuration is missing group ID: %s", propertyGroupId);
        String value = properties.getProperty(propertyKey);
        checkArgument(value != null, "Runtime configuration group %s is missing property key: %s", propertyGroupId, propertyKey);
        return value;
    }

    private static Optional<String> extractOptionalProperty(Map<String, Properties> applicationProperties, String propertyGroupId, String propertyKey)
    {
        return Optional.ofNullable(applicationProperties.get(propertyGroupId))
                .map(properties -> properties.getProperty(propertyKey));
    }

    private static void verifyJavaVersion()
    {
        if (Runtime.version().feature() != REQUIRED_JAVA_VERSION) {
            throw new RuntimeException(format("AWS Kinesis Data Analytics runtime requires Java %s (found %s)", REQUIRED_JAVA_VERSION, Runtime.version()));
        }
    }

    @VisibleForTesting
    static void run(StreamExecutionEnvironment environment, KafkaToGlueIcebergLoaderJobConfig config)
            throws Exception
    {
        verifyJavaVersion();

        environment.setRestartStrategy(new ExponentialDelayRestartStrategyConfiguration(Time.milliseconds(100), Time.seconds(30), 2.0, Time.minutes(5), 0));

        KafkaCredentials kafkaCredentials = KafkaCredentials.readFromAwsSecretsManager(
                config.getSecretsManagerRegion(),
                config.getKafkaCredentialsSecretName(),
                config.getKafkaCredentialsApiKeyField(),
                config.getKafkaCredentialsApiSecretField());

        Properties kafkaConnectionProperties = KafkaConnections.createConnectionProperties(kafkaCredentials, config.getKafkaBootstrapServers());

        // Checkpointing does not work if the task parallelism is greater than the number of Kafka partitions: https://www.mail-archive.com/user@flink.apache.org/msg43964.html
        // The below workaround can be removed once we move to Flink 1.14 which introduces the new `execution.checkpointing.checkpoints-after-tasks-finish.enabled` config option.
        // This feature is enabled by default 1.15+
        environment.enableCheckpointing(config.getFlushInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
        environment.setParallelism(
                Math.min(
                        countKafkaPartitions(kafkaConnectionProperties, config.getKafkaTopics()),
                        environment.getParallelism() > 0
                                ? environment.getParallelism()
                                : Integer.MAX_VALUE));

        TableLoader tableLoader = TableLoader.fromCatalog(
                new CustomGlueCatalogLoader(config.getS3Bucket(), config.getS3Region().id(), config.getGlueRegion().id()),
                TableIdentifier.of(Namespace.of(config.getGlueDatabase()), config.getGlueTable()));
        Table table = loadTable(tableLoader);

        KafkaSource<ConsumerRecord<byte[], byte[]>> kafkaSource = createKafkaSource(
                kafkaConnectionProperties,
                config.getKafkaTopics(),
                config.getKafkaClientGroupId(),
                config.getKafkaPartitionDiscoveryInterval());
        DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), format("KafkaSource[%s]", config.getKafkaTopics()));

        SingleOutputStreamOperator<ParsedRowData> parsingResultStream = dataStreamSource.map(new ConsumerRecordToRowDataParser(
                table.schema(),
                config.getUnnestJsonPointer(),
                config.getInputFieldNameCase().flatMap(input -> config.getOutputFieldNameCase().map(input::converterTo)),
                config.getInputFieldProjections()));

        Properties producerProperties = new Properties();
        producerProperties.putAll(kafkaConnectionProperties);
        // Note: This cannot be greater than transaction.max.timeout.ms setting in the broker (which defaults to 15 minutes in Confluent)
        producerProperties.setProperty(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                String.valueOf(config.getFlushInterval().toMillis() + TimeUnit.MINUTES.toMillis(1))); // Give minimum 1 minute extra to finish committing a transaction

        SingleOutputStreamOperator<RowData> loadingStream;
        if (config.getKafkaDeadLetterQueueTopic().isPresent()) {
            parsingResultStream.filter(value -> !value.getResult().isSuccess())
                    .addSink(new FlinkKafkaProducer<>(
                            config.getKafkaDeadLetterQueueTopic().get(),
                            new FailedParsedRowDataSerializationSchema(config.getKafkaDeadLetterQueueTopic().get()),
                            producerProperties,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

            // Filter out messages with parse errors if a dead letter queue is being used
            loadingStream = parsingResultStream.filter(value -> value.getResult().isSuccess())
                    .map(value -> value.getResult().getRowData());
        }
        else {
            // Without a dead letter queue, just let the job fail with any parse errors that are encountered
            loadingStream = parsingResultStream.map(value -> value.getResult().getRowData());
        }

        FlinkSink.forRowData(loadingStream)
                .tableLoader(tableLoader)
                .table(table)
                .append();

        environment.execute();
    }

    // Must be Serializable
    private static class ConsumerRecordToRowDataParser
            implements MapFunction<ConsumerRecord<byte[], byte[]>, ParsedRowData>
    {
        private final JsonToRowDataConverter converter;
        private final String unnestJsonPointer;
        @Nullable
        private final Converter<String, String> inputFieldNameConverter;
        private final List<InputFieldProjection> inputFieldProjections;

        private final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper objectMapper = new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper();

        public ConsumerRecordToRowDataParser(
                Schema rowSchema,
                String unnestJsonPointer,
                Optional<Converter<String, String>> inputFieldNameConverter,
                List<InputFieldProjection> inputFieldProjections)
        {
            // Use Flink's JsonToRowDataConverters as much as possible to avoid duplicating that logic
            converter = new JsonToRowDataConverters(false, false, TimestampFormat.ISO_8601)
            {
                @Override
                public JsonToRowDataConverter createConverter(LogicalType type)
                {
                    if (type.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                        // Override timestamp with TZ parsing behavior to support ISO-8601 with more time zones and zone IDs.
                        // Otherwise only UTC timezones would be supported.
                        return wrapIntoNullableConverter(jsonNode -> TimestampData.fromInstant(ZonedDateTime.parse(jsonNode.asText()).toInstant()));
                    }
                    return super.createConverter(type);
                }

                private JsonToRowDataConverter wrapIntoNullableConverter(JsonToRowDataConverter converter)
                {
                    return jsonNode -> jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()
                            ? null
                            : converter.convert(jsonNode);
                }
            }.createConverter(FlinkSchemaUtil.convert(requireNonNull(rowSchema, "rowSchema is null")));
            this.unnestJsonPointer = requireNonNull(unnestJsonPointer, "unnestJsonPointer is null");
            this.inputFieldNameConverter = requireNonNull(inputFieldNameConverter, "inputFieldNameConverter is null").orElse(null);
            this.inputFieldProjections = ImmutableList.copyOf(requireNonNull(inputFieldProjections, "inputFieldProjections is null"));
        }

        @Override
        public ParsedRowData map(ConsumerRecord<byte[], byte[]> kafkaRecord)
        {
            try {
                JsonNode base = objectMapper.readTree(kafkaRecord.value());

                // 1. Unnest the inner JSON payload from the base
                JsonNode unnestSource = base.at(unnestJsonPointer);

                // 2. Copy the unnested fields and convert the field names (recursively)
                ObjectNode processed = objectMapper.createObjectNode();
                copyConvertedFields(unnestSource, processed);

                // 3. Apply field projections relative to base
                // NOTE: field name conversion is only applied (recursively) to the extracted JSON element. The user specified field name will not have conversion applied.
                for (InputFieldProjection inputFieldProjection : inputFieldProjections) {
                    processed.set(inputFieldProjection.getFieldName(), convertFieldNames(base.at(inputFieldProjection.getJsonPointer())));
                }

                RowData rowData = (RowData) converter.convert(processed);
                return new ParsedRowData(kafkaRecord, RowDataResult.success(rowData));
            }
            catch (IOException | RuntimeException e) {
                return new ParsedRowData(kafkaRecord, RowDataResult.fail(e));
            }
        }

        private String convertFieldName(String fieldName)
        {
            return inputFieldNameConverter == null ? fieldName : inputFieldNameConverter.convert(fieldName);
        }

        private JsonNode convertFieldNames(JsonNode jsonNode)
        {
            if (inputFieldNameConverter == null) {
                return jsonNode;
            }
            if (jsonNode.isObject()) {
                ObjectNode newNode = objectMapper.createObjectNode();
                copyConvertedFields(jsonNode, newNode);
                return newNode;
            }
            if (jsonNode.isArray()) {
                ArrayNode newNode = objectMapper.createArrayNode();
                copyConvertedFields(jsonNode, newNode);
                return newNode;
            }
            return jsonNode;
        }

        private void copyConvertedFields(JsonNode source, ObjectNode destination)
        {
            Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                destination.set(convertFieldName(entry.getKey()), convertFieldNames(entry.getValue()));
            }
        }

        private void copyConvertedFields(JsonNode source, ArrayNode destination)
        {
            Iterator<JsonNode> elements = source.elements();
            while (elements.hasNext()) {
                destination.add(convertFieldNames(elements.next()));
            }
        }
    }

    // Must be Serializable
    private static class FailedParsedRowDataSerializationSchema
            implements KafkaSerializationSchema<ParsedRowData>
    {
        private static final String PROCESSING_EXCEPTION_TOPIC_HEADER = "loader.processing.exception.topic";
        private static final String PROCESSING_EXCEPTION_OFFSET_HEADER = "loader.processing.exception.offset";
        private static final String PROCESSING_EXCEPTION_CLASS_HEADER = "loader.processing.exception.class";
        private static final String PROCESSING_EXCEPTION_MESSAGE_HEADER = "loader.processing.exception.message";
        private static final String PROCESSING_EXCEPTION_STACKTRACE_HEADER = "loader.processing.exception.stacktrace";

        private final String deadLetterQueueTopic;

        public FailedParsedRowDataSerializationSchema(String deadLetterQueueTopic)
        {
            this.deadLetterQueueTopic = requireNonNull(deadLetterQueueTopic, "deadLetterQueueTopic is null");
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ParsedRowData parsedRowData, @Nullable Long timestamp)
        {
            checkArgument(!parsedRowData.getResult().isSuccess(), "Should only be serializing failed parse results");

            Throwable throwable = parsedRowData.getResult().getThrowable();

            // Apply original headers
            Map<String, byte[]> headers = new LinkedHashMap<>();
            for (Header header : parsedRowData.getInput().headers()) {
                headers.put(header.key(), header.value());
            }

            // Add extra headers
            headers.put(PROCESSING_EXCEPTION_TOPIC_HEADER, parsedRowData.getInput().topic().getBytes(UTF_8));
            headers.put(PROCESSING_EXCEPTION_OFFSET_HEADER, String.valueOf(parsedRowData.getInput().offset()).getBytes(UTF_8));
            headers.put(PROCESSING_EXCEPTION_CLASS_HEADER, throwable.getClass().getName().getBytes(UTF_8));
            headers.put(PROCESSING_EXCEPTION_MESSAGE_HEADER, Strings.nullToEmpty(throwable.getMessage()).getBytes(UTF_8));
            headers.put(PROCESSING_EXCEPTION_STACKTRACE_HEADER, getStackTraceAsString(throwable).getBytes(UTF_8));

            return new ProducerRecord<>(
                    deadLetterQueueTopic,
                    null,
                    timestamp,
                    parsedRowData.getInput().key(),
                    parsedRowData.getInput().value(),
                    headers.entrySet().stream()
                            .map(entry -> new RecordHeader(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList()));
        }
    }

    private static class ParsedRowData
    {
        private final ConsumerRecord<byte[], byte[]> input;
        private final RowDataResult result;

        public ParsedRowData(ConsumerRecord<byte[], byte[]> input, RowDataResult result)
        {
            this.input = requireNonNull(input, "input is null");
            this.result = requireNonNull(result, "result is null");
        }

        public ConsumerRecord<byte[], byte[]> getInput()
        {
            return input;
        }

        public RowDataResult getResult()
        {
            return result;
        }
    }

    private static class RowDataResult
    {
        @Nullable
        private final RowData rowData;

        @Nullable
        private final Throwable throwable;

        private RowDataResult(@Nullable RowData rowData, @Nullable Throwable throwable)
        {
            this.rowData = rowData;
            this.throwable = throwable;
            checkArgument((rowData == null) != (throwable == null), "rowData and throwable are mutually exclusive");
        }

        public static RowDataResult success(RowData rowData)
        {
            return new RowDataResult(requireNonNull(rowData, "rowData is null"), null);
        }

        public static RowDataResult fail(Throwable throwable)
        {
            return new RowDataResult(null, requireNonNull(throwable, "throwable is null"));
        }

        public boolean isSuccess()
        {
            return throwable == null;
        }

        public RowData getRowData()
        {
            if (!isSuccess()) {
                throw new RuntimeException("Data parse failure", throwable);
            }
            return rowData;
        }

        public Throwable getThrowable()
        {
            checkState(!isSuccess(), "Not a failure");
            return throwable;
        }
    }

    private static Table loadTable(TableLoader loader)
            throws IOException
    {
        loader.open();
        try {
            return loader.loadTable();
        }
        finally {
            loader.close();
        }
    }

    private static int countKafkaPartitions(Properties kafkaConnectionProperties, List<String> topics)
            throws ExecutionException, InterruptedException
    {
        int partitionCount = 0;
        try (AdminClient client = AdminClient.create(kafkaConnectionProperties)) {
            DescribeTopicsResult topicsResult = client.describeTopics(topics);
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : topicsResult.values().entrySet()) {
                partitionCount += entry.getValue().get().partitions().size();
            }
        }
        return partitionCount;
    }

    /**
     * Custom CatalogLoader that hard codes the catalog properties we care about so that we don't have to muck around
     * with magic key-value property pairs when initializing the catalog.
     */
    // Must be Serializable
    private static class CustomGlueCatalogLoader
            implements CatalogLoader
    {
        private final String s3Bucket;
        private final String s3Region;
        private final String glueRegion;

        public CustomGlueCatalogLoader(String s3Bucket, String s3Region, String glueRegion)
        {
            this.s3Bucket = requireNonNull(s3Bucket, "s3Bucket is null");
            this.s3Region = requireNonNull(s3Region, "s3Region is null");
            this.glueRegion = requireNonNull(glueRegion, "glueRegion is null");
        }

        @Override
        public Catalog loadCatalog()
        {
            Catalog glueCatalog = new GlueCatalog();
            glueCatalog.initialize("GlueIcebergCatalog", ImmutableMap.<String, String>builder()
                    .put(CatalogProperties.WAREHOUSE_LOCATION, format("s3://%s", s3Bucket))
                    .put(AwsProperties.CLIENT_FACTORY, CustomAwsClientFactory.class.getName())
                    .put(AwsProperties.GLUE_CATALOG_SKIP_ARCHIVE, String.valueOf(true)) // Archiving needs to be skipped for streaming use cases (see property docs)
                    .put(CustomAwsClientFactory.S3_REGION_PROPERTY, s3Region)
                    .put(CustomAwsClientFactory.GLUE_REGION_PROPERTY, glueRegion)
                    .build());
            return glueCatalog;
        }
    }

    // Must be Serializable
    public static class CustomAwsClientFactory
            implements AwsClientFactory
    {
        public static final String S3_REGION_PROPERTY = "s3.region";
        public static final String GLUE_REGION_PROPERTY = "glue.region";

        private String s3Region;
        private String glueRegion;

        @Override
        public S3Client s3()
        {
            return S3Client.builder()
                    .httpClientBuilder(UrlConnectionHttpClient.builder())
                    .region(Region.of(s3Region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }

        @Override
        public GlueClient glue()
        {
            return GlueClient.builder()
                    .httpClientBuilder(UrlConnectionHttpClient.builder())
                    .region(Region.of(glueRegion))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
        }

        @Override
        public KmsClient kms()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamoDbClient dynamo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Map<String, String> properties)
        {
            s3Region = requireNonNull(properties.get(S3_REGION_PROPERTY), format("%s property is not provided", S3_REGION_PROPERTY));
            glueRegion = requireNonNull(properties.get(GLUE_REGION_PROPERTY), format("%s property is not provided", GLUE_REGION_PROPERTY));
        }
    }

    private static KafkaSource<ConsumerRecord<byte[], byte[]>> createKafkaSource(Properties kafkaConnectionProperties, List<String> topics, String clientGroupId, Duration partitionDiscoveryInterval)
    {
        return KafkaSource.<ConsumerRecord<byte[], byte[]>>builder()
                .setTopics(topics)
                .setGroupId(clientGroupId)
                .setProperties(kafkaConnectionProperties)
                // Required to automatically discover new Kafka partitions that get added (otherwise, they will be ignored)
                .setProperty(PARTITION_DISCOVERY_INTERVAL_MS.key(), String.valueOf(partitionDiscoveryInterval.toMillis()))
                .setDeserializer(new KafkaRecordPassThruDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();
    }
}
