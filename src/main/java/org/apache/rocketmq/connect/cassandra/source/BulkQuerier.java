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

import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.connect.cassandra.common.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO refactor BulkQuerier following design patterns
public class BulkQuerier implements Querier {
    private final Logger log = LoggerFactory.getLogger(BulkQuerier.class);
    //TODO adjust FETCH_SIZE
    private static final int FETCH_SIZE = 2;
    private CassandraConfig config;
    private CassandraSession cassandraSession;
    private Collection<SourceDataEntry> rowList = new ArrayList<>();
    private String keyspace;
    private String table;
    private PagingState lastPagingState;
    boolean tableExhausted;
    int skipNumber;

    public BulkQuerier(CassandraConfig config, CassandraSession cassandraSession) {
        this.config = config;
        this.cassandraSession = cassandraSession;
        this.keyspace = config.getKeyspace();
        this.table = config.getTable();
    }

    public Collection<SourceDataEntry> getList() {
        return rowList;
    }

    @Override public void setisExhausted(boolean isExhausted) {
        this.tableExhausted = isExhausted;
    }

    @Override public void setpagingState(PagingState pagingState) {
        this.lastPagingState = pagingState;
    }

    @Override public void setrowUpOffset(int rowUpOffset) {
        this.skipNumber = rowUpOffset;
    }

    public void query() {
        rowList.clear();
        //TODO after extracting all data, maybe it is better to terminate task
        if (tableExhausted) {
            log.trace("No new data");
            return;
        }

        Iterator<Row> queryRes = null;
        PagingState currentPagingState = lastPagingState;
        ResultSet CurrentResultSet = null;
        int rowsOfPage = 0;
        int remaining = 0;
        try {
            log.trace("current pageInfo " + currentPagingState);
            ResultSet rs = cassandraSession.getSession().execute(QueryBuilder.select()
                .from(keyspace, table)
                .setConsistencyLevel(ConsistencyLevel.ONE)
                .setFetchSize(FETCH_SIZE)
                .setPagingState(currentPagingState)
            );
            rowsOfPage = rs.getAvailableWithoutFetching();
            queryRes = rs.iterator();
            log.trace("rowsOfPage: " + rowsOfPage);
            log.trace("skip row number: " + skipNumber);

            if (skipNumber == 0) {
                remaining = rowsOfPage;
            } else if (skipNumber < rowsOfPage) {
                Iterator<Row> iter = rs.iterator();
                remaining = rowsOfPage - skipNumber;
                while (iter.hasNext() && skipNumber > 0) {
                    Row row = iter.next();
                    log.trace("skip row : " + row);
                    skipNumber--;
                }
                queryRes = iter;
            } else if (skipNumber == rowsOfPage) {
                //TODO refactor following code
                //switch to next page, pagingState1 is next PageInfo
                PagingState pagingState1 = rs.getExecutionInfo().getPagingState();
                log.trace("switch to pagingState: " + pagingState1);
                rs = cassandraSession.getSession().execute(QueryBuilder.select()
                    .from(keyspace, table)
                    .setConsistencyLevel(ConsistencyLevel.ONE)
                    .setFetchSize(FETCH_SIZE)
                    .setPagingState(pagingState1)
                );
                if (rs == null)
                    return;
                queryRes = rs.iterator();
                rowsOfPage = remaining = rs.getAvailableWithoutFetching();
                currentPagingState = pagingState1;
            }
            CurrentResultSet = rs;
        } catch (Exception ex) {
            log.error("Cassandra skip data task error:" + ex);
        }

        log.trace("remaining :" + remaining);

        Collection<SourceDataEntry> res = new ArrayList<>();
        int rowCount = rowsOfPage - remaining;

        if (queryRes != null) {
            while (queryRes.hasNext()) {
                Row row = queryRes.next();
                rowCount++;
                log.trace("Row " + rowCount);
                Map<String, Object> position = new HashMap<>();
                if (currentPagingState == null) {
                    position.put("pagingState", "null");
                } else {
                    position.put("pagingState", currentPagingState.toString());
                }
                //rs.isExhausted() == true is the mark for the last row
                position.put("isExhausted", CurrentResultSet.isExhausted());
                position.put("rowUpOffset", rowCount);

                log.trace("position pagingState" + ((String) position.get("pagingState")));
                log.trace("position isExhausted" + ((Boolean) position.get("isExhausted")));
                log.trace("position rowUpOffset" + ((int) position.get("rowUpOffset")));
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("CassandraTablePosition", position);
                Schema schema = new Schema();
                schema.setDataSource(keyspace);
                schema.setName(table);
                schema.setFields(new ArrayList<Field>());
                //TODO optimize below two loop code
                String columnDatatype;
                String columnName;
                for (int i = 0; i < row.getColumnDefinitions().size(); i++) {
                    columnDatatype = row.getColumnDefinitions().getType(i).getName().name();
                    columnName = row.getColumnDefinitions().getName(i);

                    //Field i 有没有必要递增还是可以跟fileConnect一样设为0即可 (需要设置为i 匹配payload)
                    Field field = new Field(i, columnName, Parser.dataType2FieldType(columnDatatype));
                    schema.getFields().add(field);
                }
                DataEntryBuilder dataEntryBuilder = new DataEntryBuilder(schema);
                //currently table name is the topic name
                //TODO investigate how to cover more than one table topic names,
                // if here should use EntryType.CREATE instead of EntryType.UPDATE
                dataEntryBuilder.timestamp(System.currentTimeMillis()).queue(config.getTable())
                    .entryType(EntryType.UPDATE);
                for (int i = 0; i < row.getColumnDefinitions().size(); i++) {
                    columnName = row.getColumnDefinitions().getName(i);
                    columnDatatype = row.getColumnDefinitions().getType(i).getName().name();
                    dataEntryBuilder.putFiled(columnName, Parser.columnValue2Object(row, columnName, columnDatatype));
                }

                SourceDataEntry sourceDataEntry = dataEntryBuilder.buildSourceDataEntry(
                    ByteBuffer.wrap((config.getTable()).getBytes(Charset.defaultCharset())),
                    ByteBuffer.wrap(jsonObject.toJSONString().getBytes(Charset.defaultCharset())));
                res.add(sourceDataEntry);
                log.info("sourceDataEntry : {}", JSONObject.toJSONString(sourceDataEntry));
                if (--remaining == 0) {
                    break;
                }
            }
            rowList.addAll(res);
            //TODO investigate if here needs to close the session?
        }
    }
}
