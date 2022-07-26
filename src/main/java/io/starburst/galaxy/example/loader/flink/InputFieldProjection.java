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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

// JSON Pointer specification: https://datatracker.ietf.org/doc/html/rfc6901
public class InputFieldProjection
        implements Serializable
{
    private final String fieldName;
    private final String jsonPointer;

    public InputFieldProjection(String fieldName, String jsonPointer)
    {
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.jsonPointer = requireNonNull(jsonPointer, "jsonPointer is null");

        // Fast fail if the JSON pointer string is invalid
        JsonPointer.compile(jsonPointer);
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public String getJsonPointer()
    {
        return jsonPointer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InputFieldProjection that = (InputFieldProjection) o;
        return fieldName.equals(that.fieldName) && jsonPointer.equals(that.jsonPointer);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldName, jsonPointer);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fieldName", fieldName)
                .add("jsonPointer", jsonPointer)
                .toString();
    }
}
