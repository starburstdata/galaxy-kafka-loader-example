# Galaxy Kafka Loader Example

Near real-time data ingestion that continuously pulls JSON data from [Kafka](https://kafka.apache.org/)
and loads it into a [Starburst Galaxy](https://www.starburst.io/platform/starburst-galaxy/) managed AWS S3 data lake.

Properties:
* Exactly-once loading (no duplicates)
* Near real-time data freshness
* Fault tolerant execution
* Scalability with volume

## Operating Requirements

### Required Platforms
* [Kafka](https://kafka.apache.org/)
* AWS account
  * [S3](https://aws.amazon.com/s3/)
  * [Glue](https://aws.amazon.com/glue/)
  * [Identity and Access Management](https://aws.amazon.com/iam/)
  * [Secrets Manager](https://aws.amazon.com/secrets-manager/)
  * [Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/)
* [Starburst Galaxy](https://www.starburst.io/platform/starburst-galaxy/) account (managed Trino)

### Data Ingest Format
This loader is able to consume JSON-formatted Kafka messages and append them to a Galaxy-compatible data lake table.
Incoming Kafka payloads are expected to follow a row-based JSON object structure. For example, a single new row to append might
have the following structure:
```json
{
  "bool": true,
  "floating": 0.01,
  "integer": 1,
  "string": "value",
  "bools": [true, false, true],
  "floatings": [0.01, 0.02, 0.03],
  "integers": [1, 2, 3],
  "strings": ["a", "b", "c"],
  "string_map": {
    "k1": "v1",
    "k2": "v2"
  },
  "nested": {
    "key": "key name",
    "value": 1
  },
  "timestamp": "2022-07-26T00:41:48.046773-07:00"
}
```
Each new row to append should appear as a unique Kafka message.

**Note:** Galaxy works best with case-insensitive identifiers, and so all column and table names should be
lowercase by convention. The recommended convention is to use lowercase underscore naming (e.g. `my_column_name`).

### Job Execution Platform
This loader is executed as a [Flink](https://flink.apache.org/) job that is compatible with AWS' serverless managed
Flink platform: [Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/). Kinesis Data Analytics provides the
execution runtime with durable checkpointing and resource management, so that you do not have run this system yourself.
The job will reference secrets stored in AWS [Secrets Manager](https://aws.amazon.com/secrets-manager/) to find the necessary
Kafka credentials.

### Data Lake Output Format
The loader appends incoming rows into AWS [S3](https://aws.amazon.com/s3/) in the [Iceberg](https://iceberg.apache.org/) format.
The table schema is stored in AWS [Glue](https://aws.amazon.com/glue/). Starburst Galaxy provides the SQL engine to create, update,
query, and delete the schema and underlying data after loading.

## Environment Setup

### Create Starburst Galaxy data lake

#### Choose an AWS region to host data lake

Choose an [AWS region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) for your data lake. For the purposes of this demo, we will be using `us-east-1`.
If you don't know what to do, choose a region that matches your data source to minimize cross region network costs.

* DATA_LAKE_AWS_REGION

#### Create AWS S3 bucket for data lake

Create an S3 bucket to host your data lake data.

```shell
aws s3 mb s3://<DATA_LAKE_S3_BUCKET_NAME> --region <DATA_LAKE_AWS_REGION>

# Example:
# aws s3 mb s3://galaxy-data-lake --region us-east-1
```
NOTE: S3 bucket names need to be globally unique on the internet, so you will need to choose a unique name.

* DATA_LAKE_S3_BUCKET_NAME

#### Create AWS Glue database

Create an AWS Glue database to host the data lake table schemas.

```shell
aws glue create-database \
    --region <DATA_LAKE_AWS_REGION> \
    --database-input "{\"Name\":\"<DATA_LAKE_AWS_GLUE_DATABASE>\"}"

# Example:
# aws glue create-database \
#     --region us-east-1 \
#     --database-input "{\"Name\":\"galaxy_data_lake\"}"
```

* DATA_LAKE_AWS_GLUE_DATABASE

#### Create AWS Access Credentials for Galaxy

1. Navigate a brower to https://console.aws.amazon.com/iam.
1. Select `Users` from the left navigation bar.
1. Click the `Add users` button.
1. Provide a username (e.g. 'galaxy-user').
1. Select AWS credential type: `Access key - Programmatic access`
1. Click the `Next: Permissions` button.
1. Select the `Attach existing policies directly` tab.
1. Find and select the `AmazonS3FullAccess` policy.
   * Note: You can provide a more restricted permission set for Galaxy if desired.
1. Find and select the `AWSGlueConsoleFullAccess` policy.
   * Note: You can provide a more restricted permission set for Galaxy if desired.
1. Click `Next: Tags`.
1. Click `Next: Review`.
1. Click `Create user`.
1. Copy and save the following values somewhere:
   * Access key ID (GALAXY_AWS_ACCESS_KEY)
   * Secret access key (GALAXY_AWS_ACCESS_SECRET)

#### Add Galaxy S3 catalog

In the Galaxy UI:
1. Select `Catalogs` from the left navigation bar.
1. Click the `Configure a catalog` button.
1. Select Data Source: `Amazon S3`.
1. Provide a catalog name (e.g. '<user>_data_lake') and description.
1. Select Authentication to S3: `AWS access key`.
   * Provide the GALAXY_AWS_ACCESS_KEY and GALAXY_AWS_ACCESS_SECRET that you recorded in the previous step.
1. Select Metastore configuration: `AWS Glue`.
   * Provide DATA_LAKE_AWS_REGION as the Glue region.
   * Provide DATA_LAKE_S3_BUCKET_NAME as the default S3 bucket name.
   * Pick a default directory name of your choosing (e.g. 'data').
   * Make sure `Use the authentication details configured for S3 access` is checked.
1. Select Default table format: `Iceberg`.
1. Click the `Test connection` button.
1. Click the `Connect catalog` button.
1. Under `Full read/write access to this catalog`, make sure your current role is listed (otherwise you will not be able to create tables).
   * If you are not sure about your current role and this is just a quick test, you can add the `public` role.
1. Click the `Save access controls` button.
1. Click `Skip` for the `Add to cluster` section.

#### Mount Galaxy S3 catalog to a cluster

In the Galaxy UI:
1. Select `Clusters` from the left navigation bar.
1. Click the `Create cluster` button.
1. Provide a cluster name (e.g. '<user>-data-lake') and parameters of your choosing.
1. Make sure to add the S3 catalog you just created.
1. Make sure to select the DATA_LAKE_AWS_REGION region for your cluster.
1. Click the `Create cluster` button.
1. Your cluster should be created and automatically started.

#### Create a new table to be loaded

In the Galaxy UI:
1. Click `Query editor` from the left navigation bar.
1. Select the cluster you just created in the right drop down selector.
1. Select the catalog you just created in the right drop down selector.
1. Select DATA_LAKE_AWS_GLUE_DATABASE as the schema in the right drop down selector.
1. Create a new GALAXY_DESTINATION_TABLE_NAME table with a SQL CREATE TABLE statement, using a schema that matches your JSON payload structure.

For example, if you are going to be using the `SampleKafkaPublisher` data generator in the project,
the corresponding CREATE TABLE statement should look like:
```sql
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
```

NOTE:
* Make sure to set a format of `PARQUET` to match the loader.
* When loading real-time data, it is recommended to partition on `day(your_event_time_column)` to make future compaction and retention commands more efficient.

* GALAXY_DESTINATION_TABLE_NAME

### Create Kafka topic

Add a topic to your Kafka cluster (if it doesn't exist yet) as the queue for data ingestion.

[Optional] Add a second topic to your Kafka cluster as a dead letter queue that the loader can use to forward any malformed Kafka messages.

If you are using Confluent Cloud for managed Kafka:
1. Select an environment for your topic.
1. Select an existing cluster (or create a new cluster) for your topic.
1. Select `Topics` in the left navigation bar.
1. Click the `Create topic` button.
1. Select a new topic name and set the number of partitions.
   * If you are just doing a quick test, you only need 1 partition for now.
1. Click the `Create with defaults` button.
1. [Optional] Repeat this above process for the dead letter queue topic.

* KAFKA_TOPIC
* [OPTIONAL] DEAD_LETTER_QUEUE_KAFKA_TOPIC

### Generate Kafka API credentials

Add Kafka API access credentials for this demo.

If you are using Confluent Cloud for managed Kafka:
1. On the Cluster Overview page, select `Data Integration > API keys` on the left navigation bar.
1. Click the `Create key` button.
1. Since this is just a demo, select `Global access`.
    * Note: You can provide a more restricted permission set by selecting `Granular access` if desired. The loader requires READ permissions for the KAFKA_TOPIC topic,
      and WRITE permissions for the DEAD_LETTER_QUEUE_KAFKA_TOPIC (if present). The SampleKafkaPublisher sample data generator requires WRITE permissions for the KAFKA_TOPIC topic.
1. Click the `Next` button.
1. Give a description and click the `Download and continue` button.
1. The downloaded file will contain your API Key, API Secret, and Kafka bootstrap server.

* KAFKA_API_KEY
* KAFKA_API_SECRET
* KAFKA_BOOTSTRAP_SERVER

### Store Kafka API credentials in AWS SecretsManager

Create a secret in AWS SecretsManager to store the Kafka API access credentials.

```shell
aws secretsmanager create-secret \
    --region <DATA_LAKE_AWS_REGION> \
    --name <KAFKA_SECRET_NAME> \
    --description "Kafka API access credentials for Galaxy loader demo" \
    --secret-string "{\"apiKey\": \"<KAFKA_API_KEY>\", \"apiSecret\": \"<KAFKA_API_SECRET>\"}"

# Example
# aws secretsmanager create-secret \
#     --region us-east-1 \
#     --name "confluent/test_env/test_cluster/test_topic/loader_demo" \
#     --description "Kafka API access credentials for Galaxy loader demo" \
#     --secret-string "{\"apiKey\": \"ABCDEFGHIJKLMNOP\", \"apiSecret\": \"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ab\"}"
```

* KAFKA_SECRET_NAME

## Building the Loader

### Prebuilt
If you do not wish to build the loader JAR from scratch and want to use the stock capabilities, a prebuilt loader JAR can be downloaded:
* [v0.0.1](https://github.com/starburstdata/galaxy-kafka-loader-example/releases/tag/v0.0.1)

### From Scratch

The example Galaxy Kafka loader is a standard Maven project. Run the following command from the project root directory:
```shell
./mvnw clean install
```

After completion, the required Flink job JAR can be found at the following location:
`./target/galaxy-kafka-loader-example-1-SNAPSHOT.jar`.

## Running the Loader Locally

Running the loader locally is a great way to easily test out the loading behavior.
This should have the same logical behavior as in a production environment, although the performance and reliability characteristics will differ.

**IMPORTANT**: The loader Flink job requires a Java 11 runtime environment to due to the constraints of AWS Kinesis Data Analytics.
You will need to download and install Java 11 if you want to run this locally.
* If you are running on OSX with [homebrew](https://brew.sh/), you can use the following command to install Java 11:
```shell
brew install java11
```

The loader uses the AWS default credential provider chain on your computer to gain access to AWS resources.
The easiest way to ensure access is to update your machine-local developer AWS credentials with the context you want the loader to use.

After establishing your AWS context in your CLI...

Launch the loader locally
```shell
JAVA_HOME=<PATH_TO_JAVA_11_HOME>; ./mvnw test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.starburst.galaxy.example.loader.flink.LocalKafkaToGlueIcebergLoader \
  -Daws.s3.region=<DATA_LAKE_AWS_REGION> \
  -Daws.s3.bucket=<DATA_LAKE_S3_BUCKET_NAME> \
  -Daws.glue.region=<DATA_LAKE_AWS_REGION> \
  -Daws.glue.database=<DATA_LAKE_AWS_GLUE_DATABASE> \
  -Daws.glue.table=<GALAXY_DESTINATION_TABLE_NAME> \
  -Daws.secret.region=<DATA_LAKE_AWS_REGION> \
  -Daws.secret.kafkaCredentials.name=<KAFKA_SECRET_NAME> \
  -Dkafka.bootstrap.servers=<KAFKA_BOOTSTRAP_SERVER> \
  -Dkafka.topic=<KAFKA_TOPIC> \
  -Dkafka.dlq.topic=<DEAD_LETTER_QUEUE_KAFKA_TOPIC>

# Example for OSX with Java 11 installed:
# JAVA_HOME=$(/usr/libexec/java_home -v 11); ./mvnw test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.starburst.galaxy.example.loader.flink.LocalKafkaToGlueIcebergLoader \
#   -Daws.s3.region=us-east-1 \
#   -Daws.s3.bucket=galaxy-data-lake \
#   -Daws.glue.region=us-east-1 \
#   -Daws.glue.database=galaxy_data_lake \
#   -Daws.glue.table=test_table \
#   -Daws.secret.region=us-east-1 \
#   -Daws.secret.kafkaCredentials.name=confluent/test_env/test_cluster/test_topic/loader_demo \
#   -Dkafka.bootstrap.servers=your.kafka.server.address.com:9092 \
#   -Dkafka.topic=your-test-kafka-topic \
#   -Dkafka.dlq.topic=your-test-kafka-topic-dlq
```

[OPTIONAL] If you have been planning to use the `SampleKafkaPublisher` sample data generator, you can launch the following to insert exactly one new row into your Kafka topic:

```shell
./mvnw test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.starburst.galaxy.example.loader.flink.SampleKafkaPublisher \
  -Daws.secret.region=<DATA_LAKE_AWS_REGION> \
  -Daws.secret.kafkaCredentials.name=<KAFKA_SECRET_NAME> \
  -Dkafka.bootstrap.servers=<KAFKA_BOOTSTRAP_SERVER> \
  -Dkafka.topic=<KAFKA_TOPIC>

# Example:
# ./mvnw test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.starburst.galaxy.example.loader.flink.SampleKafkaPublisher \
#   -Daws.secret.region=us-east-1 \
#   -Daws.secret.kafkaCredentials.name=confluent/test_env/test_cluster/test_topic/publisher \
#   -Dkafka.bootstrap.servers=your.kafka.server.address.com:9092 \
#   -Dkafka.topic=your-test-kafka-topic
```

Assuming you have gotten the loader and `SampleKafkaPublisher` to both run, you should now be able to open the Galaxy query editor
and query the new rows being loaded into your GALAXY_DESTINATION_TABLE_NAME table.

```sql
SELECT * FROM <GALAXY_DESTINATION_TABLE_NAME>;

-- Example:
-- SELECT * FROM test_table;
```

NOTE:
* The default Flink flush time is 1 minute, so it may take up to 60s for the data to appear. This value can be modified to your liking.
* Remember to terminate the local loader when starting to deploying the production loader. The local loader uses the same Kafka client ID
  and thus may conflict with the production loader.

## Running Production Loader

### Store loader Flink Job in S3

Create an S3 bucket to store your Flink job JAR. AWS Kinesis Data Analytics requires the JAR to be in S3 to be able to reference it.

```shell
aws s3 mb s3://<LOADER_S3_BUCKET_NAME> --region <DATA_LAKE_AWS_REGION>

# Example:
# aws s3 mb s3://loader-flink-job --region us-east-1
```
NOTE: S3 bucket names need to be globally unique on the internet, so you will need to choose a unique name.

Now, upload the Flink job JAR to the AWS S3 bucket (after building the JAR using the prior [build instructions](#building-the-loader)).

If you have the [aws CLI tool](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html), you can run the following:
```shell
aws s3 cp ./target/galaxy-kafka-loader-example-1-SNAPSHOT.jar s3://<LOADER_JOB_S3_BUCKET>

# Example:
# aws s3 cp ./target/galaxy-kafka-loader-example-1-SNAPSHOT.jar s3://loader-flink-job
```

* LOADER_S3_BUCKET_NAME

### Launch Flink job with AWS Kinesis Data Analytics

Create a streaming application in AWS [Kinesis Data Analytics](https://console.aws.amazon.com/kinesisanalytics/home)
to run and manage the loader Flink job.

1. Navigate to AWS [Kinesis Data Analytics](https://console.aws.amazon.com/kinesisanalytics/home).
1. Make sure that your browser AWS context is set to the DATA_LAKE_AWS_REGION region.
1. Click the `Create streaming application` button.
1. For Apache Flink version: select `Apache Flink version 1.13`.
1. Provide an application name (e.g. 'loader_demo') and description for this loader job.
1. Select `Create / update IAM role` (or using an existing one if you already have a compatible one).
1. Select `Development` template for this demo, or `Production` template if you are gearing up for production.
1. Click the `Create streaming application` button at the bottom.
1. Now, on the Kinesis streaming application page for this job, click the `Configure` button.
1. Set the `Amazon S3 bucket` to the LOADER_S3_BUCKET_NAME bucket.
   * Remember to include the s3 authority in the URI (e.g. 's3://loader-flink-job').
1. Set the `Path to S3 object` to be the relative path to your Flink job JAR within the above S3 bucket (e.g. 'galaxy-kafka-loader-example-1-SNAPSHOT.jar').
1. Under the `Runtime properties` section, click the `Add new item` button to configure the streaming application with the following runtime properties:

Group ID | Key | Value
--- | --- | ---
S3Properties | region | `<DATA_LAKE_AWS_REGION>`
S3Properties | bucket | `<DATA_LAKE_S3_BUCKET_NAME>`
GlueProperties | region | `<DATA_LAKE_AWS_REGION>`
GlueProperties | database | `<DATA_LAKE_AWS_GLUE_DATABASE>`
GlueProperties | table | `<GALAXY_DESTINATION_TABLE_NAME>`
SecretsManagerProperties | region | `<DATA_LAKE_AWS_REGION>`
KafkaProperties | aws.secret.name | `<KAFKA_SECRET_NAME>`
KafkaProperties | bootstrap.servers | `<KAFKA_BOOTSTRAP_SERVER>`
KafkaProperties | client.group.id | `galaxy_example_loader`
KafkaProperties | topics | `<KAFKA_TOPIC>`

Optional properties:

Group ID | Key | Value
--- | --- | ---
KafkaProperties | dlq-topic | `<DEAD_LETTER_QUEUE_KAFKA_TOPIC>`

13. Click the `Save changes` button at the bottom of the configuration page.
1. The generated IAM role for the Flink job does not contain all the permissions needed, and so these will need to be manually added.
   On the Kinesis streaming application page for this job, click on the associated IAM role link to navigate to the IAM configuration page. Make sure to add the following policies:
   * Attach a policy that grants `GetSecretValue` to SecretsManager. The lazy way out is to attach the `SecretsManagerReadWrite` policy, but feel free to be more constrained.
   * Attach the `AmazonS3FullAccess` policy.
     * Note: You can provide a more restricted permission set if desired.
   * Attach the `AWSGlueConsoleFullAccess` policy.
     * Note: You can provide a more restricted permission set if desired.

1. On the Kinesis streaming application page for this job, click the `Run` button to start the loader job.
   * This may take several minutes to complete.
1. Monitor for any errors during this phase by clicking the `Monitoring` tab, and selecting `Logs` to watch for any error logs.
1

If everything was configured correctly, you should now be able to open the Galaxy query editor and select out the rows being loaded into your GALAXY_DESTINATION_TABLE_NAME table.
   * You can use the `SampleKafkaPublisher` data generator mentioned above to generate synthetic data.
   * Remember, the default Flink flush time is 1 minute, so it may take up to 60s for the data to appear. This value can be modified to your liking on the job configuration page.

## Data Maintenance

In a production environment, it is important to run a few periodic tasks to ensure the health of the system.

### Compaction

The loader is designed to flush frequently to decrease data latency. However, this means it is important to periodically (e.g. daily) run the following SQL command
to compact any small files generated during this process. This will help to ensure that read queries over the data can maintain optimal performance.

```sql
ALTER TABLE <GALAXY_DESTINATION_TABLE_NAME>
    EXECUTE OPTIMIZE
    WHERE CAST(<TIMESTAMP_PARTITIONING_COLUMN> AS DATE) >= CAST(now() - <SQL_TIME_INTERVAL> AS DATE)

-- Daily Example:
-- ALTER TABLE test_table
--     EXECUTE OPTIMIZE
--     WHERE CAST(timestamp AS DATE) >= CAST(now() - INTERVAL '2' DAY AS DATE)
```

If you notice read queries seeming to run slower than normal, you should check to make sure that compaction has been regularly running.

### Snapshot Expiration

The Iceberg storage format retains a different table version (or snapshot) for every write to the table. Snapshots accumulate over time and needed to be periodically
pruned to limit the size of the system metadata. In order to maintain a reasonable size, you should periodically run the following SQL command to prune out any
old snapshots that will not be used anymore.

```sql
ALTER TABLE <GALAXY_DESTINATION_TABLE_NAME>
    EXECUTE expire_snapshots(retention_threshold => '7d');

-- Daily Example:
-- ALTER TABLE test_table
--     EXECUTE expire_snapshots(retention_threshold => '7d');
```

### Retention

With any data that is being continuously ingested, there is usually a requirement for that data to be expired out of the system at some point.
This may be a means to limit total storage size or to meet common government regulatory guidelines.

Handling retention is as simple as running a periodic (e.g. daily) SQL query in Galaxy:

```sql
DELETE FROM <GALAXY_DESTINATION_TABLE_NAME>
    WHERE <TIMESTAMP_PARTITIONING_COLUMN> < date_trunc('day', now() AT TIME ZONE <TIMEZONE> - <SQL_TIME_INTERVAL>);

-- Daily Example:
-- DELETE FROM test_table
--     WHERE timestamp < date_trunc('day', now() AT TIME ZONE 'UTC' - INTERVAL '30' DAY);
```

## Migrations

### Schema Change

#### Adding a column

In Starburst Galaxy, run the following SQL query to add a column:

```sql
ALTER TABLE <GALAXY_DESTINATION_TABLE_NAME>
    ADD COLUMN <NEW_COLUMN_NAME> <NEW_COLUMN_TYPE>;

-- Example:
-- ALTER TABLE test_table
--     ADD COLUMN new_column VARCHAR;
```

Next, we need to bounce the loader to pick up this schema change:
1. Navigate to the corresponding Kinesis Data Analytics streaming application job page.
1. Click the `Configure` button at the top of the page.
1. Then, click the `Save changes` button at the bottom of the configuration page. This will trigger a job restart.
   * Although you did not change any configuration, saving the configuration will force the loader Flink job to stop and restart,
thus allowing it to pick up the new schema changes.
1. Once the loader restarts, it should start populating this new column from the Kafka data source.

Note:
* Previously loaded rows will get a value of NULL assigned for the new column.
* If the new column is introduced to the Kafka JSON message before the Galaxy schema change is detected, the new column (or any unreferenced columns) will be ignored.

#### Dropping a column

To drop a column, run the following SQL command:

```sql
ALTER TABLE <GALAXY_DESTINATION_TABLE_NAME>
    DROP COLUMN <COLUMN_NAME>;

-- Example:
-- ALTER TABLE test_table
--     DROP COLUMN new_column;
```

Next, we need to bounce the loader to pick up this schema change:
1. Navigate to the corresponding Kinesis Data Analytics streaming application job page.
1. Click the `Configure` button at the top of the page.
1. Then, click the `Save changes` button at the bottom of the configuration page. This will trigger a job restart.
    * Although you did not change any configuration, saving the configuration will force the loader Flink job to stop and restart,
      thus allowing it to pick up the new schema changes.

Note:
* During the transition period, the loader may continue to parse and write the dropped column if it still exists in the JSON payload.
  However, these values will not be visible to any of the read queries the moment the ALTER TABLE command completes.

#### Changing a column type

This is usually not recommended due to numerous type compatibilities. So just don't do this if you can help it.

### Scaling up the loader

If the ingested data volume increases, you may need to add more loader parallelism to keep up with the added workload:

1. Navigate to the corresponding Kinesis Data Analytics streaming application job page.
1. Click the `Configure` button at the top of the page.
1. Set the Parallelism parameter to a value of your choosing.
   * Note: there is no benefit to setting this number larger than the number of Kafka topic partitions.
1. Then, click the `Save changes` button at the bottom of the configuration page. The loader will restart with these new parameters.

## FAQ

### What happens if the incoming Kafka record has missing fields/columns or extra fields/columns?
Table columns that are missing in the Kafka JSON payload will be written as SQL NULL values. Columns that are
extra will be ignored.

### Why do read queries seem to be running slower now that the table loader has been running for some time?
Periodically, Iceberg table files need to be compacted to retain optimal read performance. We can do this
seamlessly in the background by running the following Trino query. See [Compaction](#compaction) for more details.

### What AWS permissions do I need to run the `aws` CLI commands through this demo?
* AmazonS3FullAccess
* AWSGlueConsoleFullAccess
* SecretsManagerReadWrite

### My Kafka topic already exists and does not match my desired table schema
The recommended practice is to format your Kafka topic payload JSON structure the same way you actually want it in the
SQL table, using the same JSON field names as the output table column names. If you can't do that for some reason (e.g. this is coming from pre-existing data),
there are a few inline JSON tweaks you can configure for the job:
1. **Unnesting** - You can use a [JSON pointer](https://datatracker.ietf.org/doc/html/rfc6901) to indicate an inner JSON object to use as your
   actual JSON payload. This is useful if you have a JSON wrapper object of some sort you want to discard (e.g. CloudEvent).
2. **Input field projections** - You can provide a list of field projections to apply to your base (unnested) JSON object that can be used to
   define (or overwrite) top level columns and values.
3. **Recursive case conversions** - You can configure an input case and desired output case format using `com.google.common.base.CaseFormat`
   enum names that will be used to recursively convert the names of all your fields. It is strongly recommended to normalize
   everything to `LOWER_UNDERSCORE` if the data is not already in that format.
   Note: Input field projection output column names will not be converted since you are configuring these names directly already.