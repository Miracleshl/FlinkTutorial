package com.miracle.sink;

import com.miracle.bean.Sensor;
import com.miracle.sourceapi.SourceApiTest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = environment.readTextFile(path);
//        DataStream<Sensor> sensorDataStream = dataStream.map(line -> {
//            String[] strings = line.split(",");
//            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
//            return sensor;
//        });
        DataStream<Sensor> sensorDataStream = environment.addSource(new SourceApiTest.MySource());
        sensorDataStream.addSink(new MySQLSinkFunction()).setParallelism(1);
        environment.execute();
    }

    public static class MySQLSinkFunction extends RichSinkFunction<Sensor> {
        private Connection conn = null;
        private PreparedStatement insert_statement = null;
        private PreparedStatement update_statement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println(1);
            super.open(parameters);
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=Hongkong","root","root");
            insert_statement = conn.prepareStatement("insert into sensor_temp (id,temp) values (?,?);");
            update_statement = conn.prepareStatement("update sensor_temp set temp = ? where id = ?;");
        }

        @Override
        public void close() throws Exception {
            System.out.println(2);
            super.close();
            insert_statement.close();
            update_statement.close();
            conn.close();
        }

        @Override
        public void invoke(Sensor value, Context context) throws Exception {
            update_statement.setDouble(1, value.getTemperature());
            update_statement.setString(2, value.getId());
            update_statement.execute();
            if (update_statement.getUpdateCount() == 0) {
                insert_statement.setString(1,value.getId());
                insert_statement.setDouble(2,value.getTemperature());
                insert_statement.execute();
            }
        }
    }
}
