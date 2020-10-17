package com.miracle.sink;

import com.miracle.sourceapi.Sensor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSinkHelper;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.junit.Test;

import java.util.Properties;

/**
 * @author QianShuang
 */
public class SinkApiTest {
    @Test
    public void sinkTest() throws Exception {
        //获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(path);
        //类型转换
        DataStream<Tuple3<String, Long, Double>> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            return Tuple3.of(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.DOUBLE));
        dataStream.print();
        dataStream.writeAsCsv("D:\\sinkTest\\out.txt");
        env.execute();
    }

    @Test
    public void sinkAddSinkTest() throws Exception {
        //获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(path);
        //类型转换
        DataStream<Sensor> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        dataStream.print();
        dataStream.addSink(StreamingFileSink.forRowFormat(
                new Path("D:\\sinkTest\\out2.txt"),
                new SimpleStringEncoder()).build());
        env.execute();
    }

    @Test
    public void sinkKafkaTest() throws Exception {
        //获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(path);
        //类型转换
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor.toString();
        });
        dataStream.print();
        dataStream.addSink(new FlinkKafkaProducer<>("192.168.137.128:9092", "sinktest", new SimpleStringSchema()));
        env.execute();
    }

    @Test
    public void sinkKafkaInAndOutTest() throws Exception {
        //获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.137.128:9092");
        properties.put("group.id", "consumer-group");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        //类型转换
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor.toString();
        });
        dataStream.print();
        dataStream.addSink(new FlinkKafkaProducer<>("192.168.137.128:9092", "sinktest", new SimpleStringSchema()));
        env.execute();
    }

    @Test
    public void sinkRedisTest() throws Exception {
        //获取环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        //获取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(path);
        //类型转换
        DataStream<Sensor> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        dataStream.print();
        FlinkJedisConfigBase conf = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.137.128").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(conf, new MyRedisMapper()));
        env.execute();
    }


    private class MyRedisMapper implements RedisMapper<Sensor> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //additionalKey 用来代表hash类型的对象key名
            RedisCommandDescription description = new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
            return description;
        }

        @Override
        public String getKeyFromData(Sensor s) {
            return s.getId();
        }

        @Override
        public String getValueFromData(Sensor s) {
            return s.getTemperature().toString();
        }
    }
}
