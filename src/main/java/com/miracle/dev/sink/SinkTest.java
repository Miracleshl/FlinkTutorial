package com.miracle.dev.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class SinkTest {
    @Test
    public void kafkaSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        environment.getConfig().setAutoWatermarkInterval(300);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        sinkStream.map(temp -> temp.toString()).addSink(new FlinkKafkaProducer<>("192.168.137.128:9092", "sinktest", new SimpleStringSchema()));
        environment.execute();
    }

    @Test
    public void kafkaToKafka() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.137.128:9092");
        properties.put("group.id", "consumer-group");
        DataStream<String> fileSource = environment.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        sinkStream.map(temp -> temp.toString()).addSink(new FlinkKafkaProducer<>("192.168.137.128:9092", "sinktest", new SimpleStringSchema()));
        environment.execute();
    }

    @Test
    public void fileSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        sinkStream.print();
        sinkStream.writeAsCsv("D:\\fileSink\\output.txt", FileSystem.WriteMode.OVERWRITE,"\n",",")
                .setParallelism(1);
//        sinkStream.writeAsText("D:\\fileSink\\output.txt", FileSystem.WriteMode.OVERWRITE)
//                .setParallelism(1);
        environment.execute("");
    }

    @Test
    public void defineFileSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        sinkStream.print();
        OutputFileConfig fileConfig = new OutputFileConfig("out", "txt");
        sinkStream.addSink(StreamingFileSink.forRowFormat(new Path("D:\\fileSink"), new SimpleStringEncoder<Tuple3<String, String, String>>()).withOutputFileConfig(fileConfig).build()).setParallelism(1);
        environment.execute("");
    }

    @Test
    public void redisSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig
                .Builder()
                .setHost("192.168.137.128")
                .setPort(6379)
                .setDatabase(1)
                .setTimeout(100)
                .build();

        sinkStream.addSink(new RedisSink<>(config, new RedisMapper<Tuple3<String, String, String>>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                RedisCommandDescription description = new RedisCommandDescription(RedisCommand.HSET, "sensor");
                return description;
            }

            @Override
            public String getKeyFromData(Tuple3<String, String, String> stringStringStringTuple3) {
                return stringStringStringTuple3.f0;
            }

            @Override
            public String getValueFromData(Tuple3<String, String, String> stringStringStringTuple3) {
                return stringStringStringTuple3.f2;
            }
        }));
        environment.execute();
    }


    @Test
    public void mysqlSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String, String, String>> sinkStream = fileSource.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], fields[2]);
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        sinkStream.addSink(new RichSinkFunction<Tuple3<String, String, String>>() {
            PreparedStatement insert = null;
            PreparedStatement update = null;
            Connection connection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                Class.forName("com.mysql.cj.jdbc.Driver");
                String url = "jdbc:mysql://localhost:3306/flink?useUnicode=true&characterEncoding=utf8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=Hongkong";
                connection = DriverManager.getConnection(url, "root", "root");
                insert = connection.prepareStatement("insert into sensor (id ,temperature) values (?,?);");
                update = connection.prepareStatement("update sensor set temperature = ? where id = ?;");
            }

            @Override
            public void close() throws Exception {
                super.close();
                update.close();
                insert.close();
                connection.close();
            }

            @Override
            public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
                update.setDouble(1, Double.valueOf(value.f2));
                update.setString(2, value.f0);
                update.execute();
                if (update.getUpdateCount() == 0) {
                    insert.setDouble(2, Double.valueOf(value.f2));
                    insert.setString(1, value.f0);
                    insert.execute();
                }
            }
        });
        environment.execute();
    }
}

