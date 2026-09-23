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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class FieldToJSONTest extends TransformationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected FieldToJSONTest(boolean isKey) {
    super(isKey);
  }

  private SinkRecord record(Schema valueSchema, Object value) {
    return new SinkRecord(TOPIC, 1, null, null, valueSchema, value, 1L);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readJsonAsMap(String json) {
    try {
      JsonNode node = MAPPER.readTree(json);
      return MAPPER.convertValue(node, Map.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void schemalessTopLevelObject() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext"));

    Map<String, Object> clientContext = new LinkedHashMap<>();
    clientContext.put("initiator", "System");
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("userId", "u1");
    value.put("clientContext", clientContext);

    SinkRecord result = this.transformation.apply(record(null, value));
    Map<String, Object> out = (Map<String, Object>) result.value();
    assertEquals("u1", out.get("userId"));
    assertInstanceOf(String.class, out.get("clientContext"));
    assertEquals("System", readJsonAsMap((String) out.get("clientContext")).get("initiator"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void schemalessNestedPath() {
    this.transformation.configure(
        ImmutableMap.of("fields", "account.registrationData.marketingData.metadata"));

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("org", "direct");
    Map<String, Object> marketingData = new LinkedHashMap<>();
    marketingData.put("promoCode", "string");
    marketingData.put("metadata", metadata);
    Map<String, Object> registrationData = new LinkedHashMap<>();
    registrationData.put("marketingData", marketingData);
    Map<String, Object> account = new LinkedHashMap<>();
    account.put("registrationData", registrationData);
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("account", account);

    SinkRecord result = this.transformation.apply(record(null, value));
    Map<String, Object> outAccount =
        (Map<String, Object>) ((Map<String, Object>) result.value()).get("account");
    Map<String, Object> outMarketing =
        (Map<String, Object>)
            ((Map<String, Object>) outAccount.get("registrationData")).get("marketingData");
    assertEquals("string", outMarketing.get("promoCode"));
    assertInstanceOf(String.class, outMarketing.get("metadata"));
    assertEquals("direct", readJsonAsMap((String) outMarketing.get("metadata")).get("org"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void schemalessMultipleFields() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext,account.metadata"));

    Map<String, Object> clientContext = new LinkedHashMap<>();
    clientContext.put("initiator", "System");
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("k", "v");
    Map<String, Object> account = new LinkedHashMap<>();
    account.put("metadata", metadata);
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("clientContext", clientContext);
    value.put("account", account);

    SinkRecord result = this.transformation.apply(record(null, value));
    Map<String, Object> out = (Map<String, Object>) result.value();
    Map<String, Object> outAccount = (Map<String, Object>) out.get("account");
    assertInstanceOf(String.class, out.get("clientContext"));
    assertInstanceOf(String.class, outAccount.get("metadata"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void schemalessAlreadyStringUntouched() {
    this.transformation.configure(ImmutableMap.of("fields", "metadata"));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("metadata", "{\"already\":\"json\"}");

    SinkRecord result = this.transformation.apply(record(null, value));
    Map<String, Object> out = (Map<String, Object>) result.value();
    assertEquals("{\"already\":\"json\"}", out.get("metadata"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void schemalessMissingAndNullIgnored() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext,account.metadata"));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("clientContext", null);
    value.put("userId", "u1");

    SinkRecord result = this.transformation.apply(record(null, value));
    Map<String, Object> out = (Map<String, Object>) result.value();
    assertNull(out.get("clientContext"));
    assertEquals("u1", out.get("userId"));
  }

  @Test
  public void schemaTopLevelStructBecomesString() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext"));

    Schema clientContextSchema = SchemaBuilder.struct()
        .field("initiator", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    Schema valueSchema = SchemaBuilder.struct()
        .field("userId", Schema.STRING_SCHEMA)
        .field("clientContext", clientContextSchema)
        .build();
    Struct clientContext = new Struct(clientContextSchema).put("initiator", "System");
    Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", clientContext);

    SinkRecord result = this.transformation.apply(record(valueSchema, value));
    Struct out = (Struct) result.value();
    assertEquals(Schema.Type.STRING, out.schema().field("clientContext").schema().type());
    assertInstanceOf(String.class, out.get("clientContext"));
    assertEquals("System", readJsonAsMap((String) out.get("clientContext")).get("initiator"));
    assertEquals("u1", out.get("userId"));
  }

  @Test
  public void schemaNestedStructBecomesString() {
    this.transformation.configure(ImmutableMap.of("fields", "account.metadata"));

    Schema metadataSchema = SchemaBuilder.struct()
        .field("org", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    Schema accountSchema = SchemaBuilder.struct()
        .field("currency", Schema.OPTIONAL_STRING_SCHEMA)
        .field("metadata", metadataSchema)
        .build();
    Schema valueSchema = SchemaBuilder.struct()
        .field("userId", Schema.STRING_SCHEMA)
        .field("account", accountSchema)
        .build();
    Struct metadata = new Struct(metadataSchema).put("org", "direct");
    Struct account = new Struct(accountSchema).put("currency", "EUR").put("metadata", metadata);
    Struct value = new Struct(valueSchema).put("userId", "u1").put("account", account);

    SinkRecord result = this.transformation.apply(record(valueSchema, value));
    Struct out = (Struct) result.value();
    Struct outAccount = (Struct) out.get("account");
    assertEquals(Schema.Type.STRUCT, out.schema().field("account").schema().type());
    assertEquals(Schema.Type.STRING, outAccount.schema().field("metadata").schema().type());
    assertEquals("EUR", outAccount.get("currency"));
    assertEquals("direct", readJsonAsMap((String) outAccount.get("metadata")).get("org"));
  }

  @Test
  public void schemaNullLeafStaysNull() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext"));

    Schema clientContextSchema = SchemaBuilder.struct()
        .field("initiator", Schema.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();
    Schema valueSchema = SchemaBuilder.struct()
        .field("userId", Schema.STRING_SCHEMA)
        .field("clientContext", clientContextSchema)
        .build();
    Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", null);

    SinkRecord result = this.transformation.apply(record(valueSchema, value));
    Struct out = (Struct) result.value();
    assertEquals(Schema.Type.STRING, out.schema().field("clientContext").schema().type());
    assertNull(out.get("clientContext"));
  }

  @Test
  public void nullValuePassedThrough() {
    this.transformation.configure(ImmutableMap.of("fields", "clientContext"));
    SinkRecord result = this.transformation.apply(record(null, null));
    assertNull(result.value());
  }

  @Test
  public void configureRequiresFields() {
    assertThrows(Exception.class, () -> this.transformation.configure(ImmutableMap.of()));
  }

  public static class ValueTest<R extends ConnectRecord<R>> extends FieldToJSONTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transformation<SinkRecord> create() {
      return new FieldToJSON.Value<>();
    }
  }
}
