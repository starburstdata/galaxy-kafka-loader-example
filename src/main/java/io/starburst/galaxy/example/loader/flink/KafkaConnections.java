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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.List;
import java.util.Properties;

import static java.lang.String.format;

public final class KafkaConnections
{
    private KafkaConnections() {}

    public static Properties createConnectionProperties(KafkaCredentials credentials, List<String> kafkaBootstrapServers)
    {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", kafkaBootstrapServers));

        properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
                credentials.getApiKey(),
                credentials.getApiSecret()));
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");

        return properties;
    }
}
