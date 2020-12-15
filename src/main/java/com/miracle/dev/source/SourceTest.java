package com.miracle.dev.source;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class SourceTest {
    @Test
    public void collectionSource() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        List<Sensor> list = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").timestamp(1L).temperature(1.0).build());
        list.add(Sensor.builder().id("2").timestamp(2L).temperature(2.0).build());
        list.add(Sensor.builder().id("3").timestamp(3L).temperature(3.0).build());
        DataStream<Sensor> listSource = environment.fromCollection(list);
        listSource.print("list source");
        //可以再次读取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        fileSource.print("file source:");
        environment.execute("Source Test!");
    }

    @Test
    public void fileSource() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        fileSource.print("file source:");
        environment.execute("Source Test!");
    }

    @Test
    public void kafkaSource() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(8);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.137.128:9092");
        properties.put("group.id", "consumer-group");
        String topic = "sensor";
        DeserializationSchema deserializationSchema = new SimpleStringSchema();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, deserializationSchema, properties);
        DataStream<String> kafkaSource = environment.addSource(kafkaConsumer);
        kafkaSource.print("kafka source:");
        environment.execute("Source Test!");
    }

    @Test
    public void defineSource() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<Sensor> mySource = new MySource();
        DataStream<Sensor> defineSource = environment.addSource(mySource);
        defineSource.print();
        environment.execute("Define Source");
    }

    private class MySource implements SourceFunction<Sensor> {
        private Boolean running = Boolean.TRUE;

        @Override
        public void run(SourceContext<Sensor> ctx) throws Exception {
            Random random = new Random();
            List<Sensor> list = new ArrayList(3);
            list.add(Sensor.builder().id("1").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            list.add(Sensor.builder().id("2").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            list.add(Sensor.builder().id("3").temperature(random.nextDouble() * 100).timestamp(System.currentTimeMillis()).build());
            while (running) {
                Thread.sleep(5000);
                long timestamp = System.currentTimeMillis();
                list.forEach(sensor -> {
                    sensor.setTemperature(sensor.getTemperature() + random.nextGaussian());
                    sensor.setTimestamp(timestamp);
                    ctx.collect(sensor);
                });
            }
        }

        @Override
        public void cancel() {
            running = Boolean.FALSE;
        }
    }
}
