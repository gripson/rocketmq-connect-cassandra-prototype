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

package org.apache.rocketmq.connect.cassandra.source;

import io.openmessaging.KeyValue;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.connect.cassandra.common.CassandraUtils;

public class CassandraConfig {
    //TODO add more configuration options
    public static final String CONTACTPOINTS = "contactPoints";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "table";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    private String[] contactPoints;
    private int port;
    private String username;
    //TODO replace String with char to improve the security
    private String password;
    private String keyspace;
    private String table;
    private boolean securityEnabled;
    private String mode;

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add(KEYSPACE);
            add(TABLE);
            add(USERNAME);
            add(PASSWORD);
            add(CONTACTPOINTS);
        }
    };

    public void load(KeyValue props) {
        CassandraUtils.properties2Object(props, this);
    }

    public String[] getContactPoints() {
        return contactPoints;
    }

    public void setContactPoints(String contactPoints) {
        //TODO if contactPoints without "," need to be checked
        String[] val1 = contactPoints.split(",");
        String[] val2 = new String[val1.length];
        for (int i = 0; i < val1.length; i++) {
            val2[i] = val1[i].trim();
        }
        this.contactPoints = val2;
    }

    public boolean isSecurityEnabled() {
        return securityEnabled;
    }

    public void setSecurityEnabled(boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public static Set<String> getRequestConfig() {
        return REQUEST_CONFIG;
    }
}
