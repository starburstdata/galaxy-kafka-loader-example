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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import static java.util.Objects.requireNonNull;

public class KafkaCredentials
{
    private final String apiKey;
    private final String apiSecret;

    public KafkaCredentials(String apiKey, String apiSecret)
    {
        this.apiKey = requireNonNull(apiKey, "apiKey is null");
        this.apiSecret = requireNonNull(apiSecret, "apiSecret is null");
    }

    public String getApiKey()
    {
        return apiKey;
    }

    public String getApiSecret()
    {
        return apiSecret;
    }

    public static KafkaCredentials readFromAwsSecretsManager(Region secretsRegion, String secretName, String apiKeyField, String apiSecretField)
    {
        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(secretsRegion)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            GetSecretValueResponse response = client.getSecretValue(GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build());
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode jsonNode = objectMapper.readTree(response.secretString());
                return new KafkaCredentials(
                        jsonNode.get(apiKeyField).asText(),
                        jsonNode.get(apiSecretField).asText());
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
