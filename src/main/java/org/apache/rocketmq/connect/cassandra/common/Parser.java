/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.cassandra.common;

import com.datastax.driver.core.Row;
import io.openmessaging.connector.api.data.FieldType;

public class Parser {
    //TODO below all data types need to be tested, add collection data types
    public static Object columnValue2Object(Row row, String columnName, String columnDataType) {
        try{
            switch (columnDataType) {
                case "TINYINT":
                case "SMALLINT":
                case "MEDIUMINT":
                case "INT":
                    return row.getInt(columnName);
                case "BIGINT":
                    return row.getLong(columnName);
                case "FLOAT":
                    return row.getFloat(columnName);
                case "DOUBLE":
                    return row.getDouble(columnName);
                case "UUID":
                case "TIMEUUID":
                    return row.getUUID(columnName);
                case "TINYTEXT":
                case "TEXT":
                case "MEDIUMTEXT":
                case "LONGTEXT":
                case "VARCHAR":
                case "CHAR":
                    return row.getString(columnName);
                case "DATE":
                case "DATETIME":
                case "TIMESTAMP":
                case "TIME":
                case "YEAR":
                    return row.getTimestamp(columnName);
            }
            throw new UnsupportedOperationException(String.format("Unsupported type %s", columnDataType));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static FieldType dataType2FieldType(String dataType) {
        switch (dataType) {
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
                return FieldType.INT32;
            case "BIGINT":
            case "COUNTER":
                return FieldType.INT64;
            case "FLOAT":
                return FieldType.FLOAT32;
            case "DOUBLE":
                return FieldType.FLOAT64;
            case "BOOLEAN":
                return FieldType.BOOLEAN;
            case "VARINT":
                return FieldType.BIG_INTEGER;
            case "ASCII":
            case "INET":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "VARCHAR":
            case "CHAR":
            case "UUID":
            case "TIMEUUID":
                return FieldType.STRING;
            case "DATE":
            case "DATETIME":
            case "TIMESTAMP":
            case "TIME":
            case "YEAR":
                return FieldType.DATETIME;
            case "ENUM":
            case "SET":
                return null;
            case "MAP":
                return FieldType.MAP;
            case "LIST":
                return FieldType.ARRAY;
            case "DECIMAL":
            default:
                return FieldType.BYTES;
        }
    }
}
