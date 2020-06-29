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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSessionFactory {
    private static Logger log = LoggerFactory.getLogger(CassandraSessionFactory.class);
    private static Session instance = null;
    private static Cluster cluster = null;

    public static CassandraSession newSession(CassandraConfig cassandraConfig) {
        if (null == instance) {
            Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(cassandraConfig.getContactPoints())
                .withPort(cassandraConfig.getPort())
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
                .withCredentials(cassandraConfig.getUsername(), cassandraConfig.getPassword())
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);

            cluster = clusterBuilder.build();
            instance = cluster.newSession();
            log.info("Create Cassandra session successfully");
        }

        return new CassandraSession(cassandraConfig, cluster, instance);
    }
}
