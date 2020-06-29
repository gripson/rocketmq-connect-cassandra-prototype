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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.PagingState;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.exception.ConnectException;
import io.openmessaging.connector.api.source.SourceTask;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(CassandraSourceTask.class);
    private CassandraConfig cassandraConfig;
    private CassandraSession cassandraSession;
    private Querier querier;

    public CassandraSourceTask() {
        this.cassandraConfig = new CassandraConfig();
    }

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    //TODO refactor poll method with better solution
    @Override public Collection<SourceDataEntry> poll() {
        log.info("Start polling");
        if (this.isRunning.get()) {
            Collection<SourceDataEntry> records = null;
            if (querier != null) {
                try {
                    //TODO read.sleep(10000) here is using for avoid fast polling action, because runtime store sourcedata position every 10000ms
                    // replace thread.sleep() with better solution or enhance runtime
                    try {
                        System.out.println(Thread.currentThread().getName());
                        Thread.currentThread();
                        Thread.sleep(10000);//毫秒
                    } catch (Exception e) {
                        throw e;
                    }
                    setOffset2QueryFromStorage(querier, cassandraConfig.getTable());
                    querier.query();
                    if (querier.getList().size() > 0) {
                        records = new ArrayList<>();
                        records.addAll(querier.getList());
                        log.info("DataEntry poll successfully,{}", JSONObject.toJSONString(records));
                    }
                } catch (Exception e) {
                    log.error("Cassandra source task polls error, current config:" + JSON.toJSONString(cassandraConfig), e);
                }
            }
            return records;
        }
        return null;
    }

    private void setOffset2QueryFromStorage(Querier querier, String tableName) {
        ByteBuffer positionInfo;
        positionInfo = this.context.positionStorageReader()
            .getPosition(ByteBuffer.wrap(tableName.getBytes(Charset.defaultCharset())));

        if (positionInfo != null) {
            String positionJson = new String(positionInfo.array(), Charset.defaultCharset());
            JSONObject jsonObject = JSONObject.parseObject(positionJson);

            //TODO change "CassandraTablePosition" to TableContants.NEXT_POSITION
            Object lastRecordedOffset = jsonObject.get("CassandraTablePosition");
            if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Map))
                throw new ConnectException(-1, "Offset position is the incorrect type");
            if (lastRecordedOffset != null) {
                //TODO rename skipRowInfo
                Map<String, Object> skipRowInfo = (Map) lastRecordedOffset;

                log.info("isExhausted " + (boolean) skipRowInfo.get("isExhausted"));
                log.info("pagingState " + (String) skipRowInfo.get("pagingState"));
                log.info("rowUpOffset " + (int) skipRowInfo.get("rowUpOffset"));

                querier.setisExhausted((boolean) skipRowInfo.get("isExhausted"));
                if (((String) skipRowInfo.get("pagingState")).equals("null")) {
                    querier.setpagingState(null);
                } else {
                    querier.setpagingState(PagingState.fromString((String) skipRowInfo.get("pagingState")));
                }
                querier.setrowUpOffset((int) skipRowInfo.get("rowUpOffset"));
            }
        }

    }

    @Override public void start(KeyValue props) {
        try {
            cassandraConfig.load(props);
            cassandraSession = CassandraSessionFactory.newSession(cassandraConfig);
            this.isRunning.set(true);
            log.info("Initialize Cassandra session successfully");
        } catch (Exception e) {
            log.error("Cannot start Cassandra Source Task because of configuration error{}", e);
        }

        String mode = cassandraConfig.getMode();
        try {
            if (mode.equalsIgnoreCase("bulk")) {
                querier = new BulkQuerier(cassandraConfig, cassandraSession);
            } else if (mode.equalsIgnoreCase("incrementing")) {
                // TODO  querier = new TimestampIncrementingQuerier();
            } else if (mode.equalsIgnoreCase("timestamp")) {
                // TODO  querier = new TimestampQuerier();
            } else if (mode.equalsIgnoreCase("timestampIncrementing")) {
                // TODO  querier = new TimestampIncrementingQuerier();
            } else {
                throw new UnsupportedOperationException(String.format("Unsupported mode %s", mode));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override public void stop() {
        try {
            this.isRunning.set(false);
            if (cassandraSession != null) {
                cassandraSession.close();
                log.info("Cassandra source task connection is closed.");
            }
        } catch (Throwable e) {
            log.warn("source task stop error while closing connection to {}", "Cassandra", e);
        }
    }

    @Override public void pause() {

    }

    @Override public void resume() {

    }
}
