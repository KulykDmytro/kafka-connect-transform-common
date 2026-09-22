/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.google.common.base.Charsets;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class FieldToJSON<R extends ConnectRecord<R>> extends BaseTransformation<R> {
  private static final Logger log = LoggerFactory.getLogger(FieldToJSON.class);

  FieldToJSONConfig config;
  final JsonConverter converter = new JsonConverter();
  final Map<Schema, Schema> schemaCache = new HashMap<>();
  List<String[]> fieldPaths;

  @Override
  public ConfigDef config() {
    return FieldToJSONConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FieldToJSONConfig(settings);
    Map<String, Object> settingsClone = new LinkedHashMap<>(settings);
    settingsClone.put(FieldToJSONConfig.SCHEMAS_ENABLE_CONFIG, this.config.schemasEnable);
    this.converter.configure(settingsClone, false);
    this.fieldPaths = new ArrayList<>(this.config.fields.size());
    for (String field : this.config.fields) {
      this.fieldPaths.add(field.split("\\."));
    }
  }

  private String toJsonString(Schema schema, Object value) {
    final byte[] buffer = this.converter.fromConnectData("dummy", schema, value);
    return new String(buffer, Charsets.UTF_8);
  }

  @Override
  protected SchemaAndValue processMap(R record, Map<String, Object> input) {
    Map<String, Object> updated = new LinkedHashMap<>(input);
    for (String[] path : this.fieldPaths) {
      stringifyInMap(updated, path, 0);
    }
    return new SchemaAndValue(null, updated);
  }

  @SuppressWarnings("unchecked")
  private void stringifyInMap(Map<String, Object> node, String[] path, int index) {
    final String key = path[index];
    final boolean leaf = index == path.length - 1;
    if (!node.containsKey(key)) {
      return;
    }
    final Object child = node.get(key);
    if (null == child) {
      return;
    }
    if (leaf) {
      if (child instanceof String) {
        return;
      }
      node.put(key, toJsonString(null, child));
      return;
    }
    if (!(child instanceof Map)) {
      return;
    }
    Map<String, Object> childMap = new LinkedHashMap<>((Map<String, Object>) child);
    node.put(key, childMap);
    stringifyInMap(childMap, path, index + 1);
  }

  @Override
  protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
    final Schema updatedSchema = this.schemaCache.computeIfAbsent(
        inputSchema, s -> buildFieldSchema(s, 0));
    final Struct updatedValue = buildFieldStruct(input, updatedSchema);
    return new SchemaAndValue(updatedSchema, updatedValue);
  }

  private boolean matchesLeaf(String fieldName, int depth) {
    for (String[] path : this.fieldPaths) {
      if (depth == path.length - 1 && path[depth].equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  private boolean matchesIntermediate(String fieldName, int depth) {
    for (String[] path : this.fieldPaths) {
      if (depth < path.length - 1 && path[depth].equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  private Schema buildFieldSchema(Schema schema, int depth) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    if (null != schema.name()) {
      builder.name(schema.name());
    }
    if (null != schema.version()) {
      builder.version(schema.version());
    }
    if (null != schema.doc()) {
      builder.doc(schema.doc());
    }
    if (schema.isOptional()) {
      builder.optional();
    }
    for (Field field : schema.fields()) {
      Schema fieldSchema = field.schema();
      if (matchesLeaf(field.name(), depth)) {
        fieldSchema = field.schema().isOptional()
            ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      } else if (matchesIntermediate(field.name(), depth)
          && field.schema().type() == Schema.Type.STRUCT) {
        fieldSchema = buildFieldSchema(field.schema(), depth + 1);
      }
      builder.field(field.name(), fieldSchema);
    }
    return builder.build();
  }

  private Struct buildFieldStruct(Struct source, Schema targetSchema) {
    final Struct target = new Struct(targetSchema);
    for (Field field : source.schema().fields()) {
      final Object sourceValue = source.get(field);
      final Field targetField = targetSchema.field(field.name());
      if (null == sourceValue) {
        target.put(targetField, null);
      } else if (targetField.schema().type() == Schema.Type.STRING
          && field.schema().type() != Schema.Type.STRING) {
        target.put(targetField, toJsonString(field.schema(), sourceValue));
      } else if (targetField.schema().type() == Schema.Type.STRUCT
          && field.schema().type() == Schema.Type.STRUCT) {
        target.put(targetField, buildFieldStruct((Struct) sourceValue, targetField.schema()));
      } else {
        target.put(targetField, sourceValue);
      }
    }
    return target;
  }

  @Title("FieldToJson(Key)")
  @Description("This transformation converts one or more fields to their JSON string representation " +
      "by way of the JsonConverter built into Kafka Connect, leaving the rest of the record intact. " +
      "Nested fields are addressed using dot notation.")
  @DocumentationTip("This transformation is used to manipulate fields in the Key of the record.")
  public static class Key<R extends ConnectRecord<R>> extends FieldToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          transformed.schema(),
          transformed.value(),
          r.valueSchema(),
          r.value(),
          r.timestamp()
      );
    }
  }

  @Title("FieldToJson(Value)")
  @Description("This transformation converts one or more fields to their JSON string representation " +
      "by way of the JsonConverter built into Kafka Connect, leaving the rest of the record intact. " +
      "Nested fields are addressed using dot notation.")
  public static class Value<R extends ConnectRecord<R>> extends FieldToJSON<R> {

    @Override
    public R apply(R r) {
      final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

      return r.newRecord(
          r.topic(),
          r.kafkaPartition(),
          r.keySchema(),
          r.key(),
          transformed.schema(),
          transformed.value(),
          r.timestamp()
      );
    }
  }

}
