package com.miracle.dev.workdemo.sqlway;

import com.miracle.bean.Sensor;
import com.miracle.sourceapi.SourceApiTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class SqlWorkDemo {
    @Test
    public void workDemo(){
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(environment);
        DataStream<Sensor> dataStream = environment.addSource(new SourceApiTest.MySource());
        String sql = "CREATE TABLE fs_table (\n" +
                "  id STRING,\n" +
                "  ts BIGINT,\n" +
                "  temperature DOUBLE" +
                ") PARTITIONED BY (id) WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='file:///D:/flinkSqlSink/abc',\n" +
                "  'sink.rolling-policy.file-size'='128M',\n" +
                "  'sink.rolling-policy.rollover-interval'='30m',\n" +
                "  'sink.rolling-policy.check-interval'='1m',\n" +
                "  'csv.field-delimiter'='0x01',\n" +
                "  'format'='csv'\n" +
                ")";
        tEnv.executeSql(sql);
        tEnv.createTemporaryView("sensors", dataStream);
        String insertSql = "insert into  fs_table SELECT userId, amount, " +
                " id, timestamp, temperature FROM sensors";

        tEnv.executeSql(insertSql);
    }
}
