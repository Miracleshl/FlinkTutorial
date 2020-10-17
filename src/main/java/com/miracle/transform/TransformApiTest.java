package com.miracle.transform;

import com.miracle.sourceapi.Sensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author QianShuang
 */
public class TransformApiTest {
    @Test
    public void transform() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(path);
        //样例类转换
        DataStream<Sensor> sensorDataStream = dataStream.map(data -> {
            String[] strings = data.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        //min默认输出的是第一条数据的非比较字段temperature
        //minBy会输出当前选出的数据的字段
        //分组聚合输出当前传感器的最小值
        DataStream<Sensor> result = sensorDataStream.keyBy("id").minBy("temperature");
        result.print();
        env.execute("min temperature");
    }

    @Test
    public void transformReduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(path);
        //样例类转换
        DataStream<Sensor> sensorDataStream = dataStream.map(data -> {
            String[] strings = data.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        //分组聚合输出当前传感器的最小值,时间戳的最大值
        DataStream<Sensor> result = sensorDataStream
                .keyBy("id")
                .reduce((currData, newData) -> {
                    Sensor temp = new Sensor();
                    temp.setId(newData.getId());
                    temp.setTemperature(Math.min(currData.getTemperature(), newData.getTemperature()));
                    temp.setTimestamp(Math.max(currData.getTimestamp(), newData.getTimestamp()));
                    return temp;
                });
        result.print();
        env.execute("min temperature");
    }

    @Test
    public void transformSplit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(path);
        //样例类转换
        DataStream<Sensor> sensorDataStream = dataStream.map(data -> {
            String[] strings = data.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        //将传感器数据分为低温高温两个流 打上标签通过select选出来带标签的数据
        SplitStream<Sensor> splitStream = sensorDataStream.split(data -> {
            Set<String> tags = new HashSet<>();
            if (data.getTemperature() > 30) {
                tags.add("high");
            } else {
                tags.add("low");
            }
            if (data.getId().equals("sensor_1")) {
                tags.add("first");
            }
            //可以打多个标签
            return tags;
        });
        DataStream<Sensor> highStream = splitStream.select("high");
        DataStream<Sensor> lowStream = splitStream.select("low");
        DataStream<Sensor> allStream = splitStream.select("high", "low");
        DataStream<Sensor> firstStream = splitStream.select("first");
        highStream.print("high");
        lowStream.print("low");
        allStream.print("all");
        firstStream.print("first");
        env.execute("min temperature");
    }

    @Test
    public void transformConnect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> dataStream = env.readTextFile(path);
        //样例类转换
        DataStream<Sensor> sensorDataStream = dataStream.map(data -> {
            String[] strings = data.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        });
        //将传感器数据分为低温高温两个流 打上标签通过select选出来带标签的数据
        SplitStream<Sensor> splitStream = sensorDataStream.split(data -> {
            Set<String> tags = new HashSet<>();
            if (data.getTemperature() > 30) {
                tags.add("high");
            } else {
                tags.add("low");
            }
            if (data.getId().equals("sensor_1")) {
                tags.add("first");
            }
            //可以打多个标签
            return tags;
        });
        DataStream<Sensor> highStream = splitStream.select("high");
        DataStream<Sensor> lowStream = splitStream.select("low");
        DataStream<Tuple2<String,Double>> warningStream = highStream.map(sensor -> Tuple2.of(sensor.getId(),sensor.getTemperature()))
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE));
        ConnectedStreams<Tuple2<String,Double>,Sensor> connectedStreams = warningStream.connect(lowStream);
        DataStream<String> result = connectedStreams.map(new CoMapFunction<Tuple2<String,Double>,Sensor,String>() {
            @Override
            public String map1(Tuple2<String,Double> tuple2) {
                return tuple2.f0+" "+tuple2.f1+" warning";
            }

            @Override
            public String map2(Sensor sensor) {
                return sensor.getId()+" "+sensor.getTemperature()+" health";
            }
        });
        result.print();
        env.execute("min temperature");
    }
}
