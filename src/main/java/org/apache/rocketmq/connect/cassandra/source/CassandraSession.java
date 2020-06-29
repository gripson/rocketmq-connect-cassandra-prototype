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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSession {
    private final Logger log = LoggerFactory.getLogger(CassandraSession.class);

    private CassandraConfig cassandraConfig;
    private Cluster cluster;
    private Session session;

    public CassandraSession(CassandraConfig cassandraConfig, Cluster cluster, Session session) {
        this.cassandraConfig = cassandraConfig;
        this.cluster = cluster;
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    public void close() throws IOException {
        this.session.close();
        this.cluster.close();
        log.info("Close session and cluster connection successfully");
    }
}
