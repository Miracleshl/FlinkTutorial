package com.miracle.sourceapi;

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

/**
 * @author QianShuang
 */
public class SourceApiTest {
    @Test
    public void sourceApiFromCollection() throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建基础数据
        List<Sensor> dataList = new ArrayList<>(3);
        dataList.add(new Sensor("sensor_1",1547718199L,35.8));
        dataList.add(new Sensor("sensor_6",1547718201L,15.4));
        dataList.add(new Sensor("sensor_7",1547718202L,6.7));
        dataList.add(new Sensor("sensor_10",1547718205L,38.1));
        //从集合中加载数据
        DataStream<Sensor> dataStream = env.fromCollection(dataList);
        dataStream.print("stream1:").setParallelism(1);
        env.execute("source test");
    }
    @Test
    public void sourceApiFromFile() throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件中加载数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(path);
        dataStream.print("stream1:");
        env.execute("source test");
    }
    @Test
    public void sourceApiFromKafka() throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从Kafka中加载数据
        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.137.128:9092");
        properties.put("group.id","consumer-group");
        DataStream<String> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        dataStream.print("stream1:");
        env.execute("source test");
    }

    @Test
    public void sourceApiFromMySource() throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从自定义数据源中加载数据
        MySource mySource = new MySource();
        DataStream<Sensor> dataStream = env.addSource(mySource);
        dataStream.print("stream1:");
        env.execute("source test");
    }

    public static class MySource implements SourceFunction<Sensor> {
        private Boolean running = Boolean.TRUE;

        @Override
        public void run(SourceContext<Sensor> sourceContext) throws Exception {
            //准备基础数据
            Random random = new Random();
            List<Sensor> sensorList = new ArrayList<>(10);

            for (int i = 0; i < 3; i++) {
                sensorList.add(new Sensor("sensor_"+i,null,random.nextDouble()*100));
            }
            while (running){
                System.out.println(running);
                Long timestamp = System.currentTimeMillis();
                sensorList.forEach(sensor -> {
                    sensor.setTemperature(sensor.getTemperature()+random.nextGaussian());
                    sensor.setTimestamp(timestamp);
                    //发送数据
                    sourceContext.collect(sensor);
                });
                //睡1秒
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.running = Boolean.FALSE;
        }
    }
}
