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

import com.google.common.reflect.TypeToken;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Simple {@code ConsumerRecord<byte[], byte[]>} passthrough deserializer that makes no attempt to deserialize
 * anything, and thus cannot throw any Exceptions.
 */
public class KafkaRecordPassThruDeserializationSchema
        implements KafkaRecordDeserializationSchema<ConsumerRecord<byte[], byte[]>>
{
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ConsumerRecord<byte[], byte[]>> out)
    {
        out.collect(record);
    }

    @SuppressWarnings("unchecked")
    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType()
    {
        return (TypeInformation<ConsumerRecord<byte[], byte[]>>) TypeExtractor.createTypeInfo(new TypeToken<ConsumerRecord<byte[], byte[]>>() {}.getType());
    }
}
