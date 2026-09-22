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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public class FieldToJSONConfig extends AbstractConfig {
  public final boolean schemasEnable;
  public final List<String> fields;

  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final String SCHEMAS_ENABLE_DOC = "Flag to determine if the JSON data should include the schema.";
  public static final String FIELDS_CONFIG = "fields";
  public static final String FIELDS_DOC = "Comma-separated list of field paths whose value is converted to a "
      + "JSON string, using dot notation for nested fields (e.g. metadata,parent.child).";


  public FieldToJSONConfig(Map<String, ?> settings) {
    super(config(), settings);
    this.schemasEnable = getBoolean(SCHEMAS_ENABLE_CONFIG);
    this.fields = getList(FIELDS_CONFIG);
    if (this.fields == null || this.fields.isEmpty()) {
      throw new ConfigException(FIELDS_CONFIG, this.fields, "At least one field path must be configured.");
    }
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(SCHEMAS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN)
                .documentation(SCHEMAS_ENABLE_DOC)
                .defaultValue(false)
                .importance(ConfigDef.Importance.MEDIUM)
                .build()
        ).define(
            ConfigKeyBuilder.of(FIELDS_CONFIG, ConfigDef.Type.LIST)
                .documentation(FIELDS_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        );
  }

}
