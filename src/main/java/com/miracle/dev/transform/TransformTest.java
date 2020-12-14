package com.miracle.dev.transform;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TransformTest {
    @Test
    public void simpleTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        List<Sensor> list = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(4L).build());

        DataStream<Sensor> listSource = environment.fromCollection(list);
        listSource.flatMap((line, collector) -> collector.collect(line), Types.POJO(Sensor.class))
                .filter(sensor -> Objects.equals(sensor.getId(), "1"))
                .keyBy(sensor -> sensor.getId())
                .max("temperature")
                .print();
        environment.execute();
    }

    @Test
    public void reduceTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        List<Sensor> list = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list.add(Sensor.builder().id("1").temperature(101D).timestamp(4L).build());
        list.add(Sensor.builder().id("1").temperature(99D).timestamp(2L).build());

        DataStream<Sensor> listSource = environment.fromCollection(list);
        listSource.flatMap((line, collector) -> collector.collect(line), Types.POJO(Sensor.class))
                .filter(sensor -> Objects.equals(sensor.getId(), "1"))
                .keyBy(sensor -> sensor.getId())
                .reduce((lasted, current) -> {
                    lasted.setTimestamp(Long.max(lasted.getTimestamp(), current.getTimestamp()));
                    lasted.setTemperature(Double.min(lasted.getTemperature(), current.getTemperature()));
                    return lasted;
                })
                .print();
        environment.execute();
    }

    @Test
    public void splitTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        List<Sensor> list = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list.add(Sensor.builder().id("1").temperature(101D).timestamp(4L).build());
        list.add(Sensor.builder().id("1").temperature(99D).timestamp(2L).build());

        DataStream<Sensor> listSource = environment.fromCollection(list);
        SplitStream<Sensor> splitStream = listSource.split(sensor -> sensor.getTemperature() > 100 ? Collections.singleton("hight") : Collections.singleton("low"));
        splitStream.select("hight").print("hight:");
        splitStream.select("low").print("low:");
        splitStream.select("low", "hight").print("all:");
        environment.execute();
    }

    @Test
    public void connectTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        List<Sensor> list = new ArrayList<>(3);
        List<String> list2 = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list2.add("2");
        list2.add("3");

        DataStream<Sensor> listSource = environment.fromCollection(list);
        DataStream<String> listSource2 = environment.fromCollection(list2);
        ConnectedStreams<Sensor, String> connect = listSource.connect(listSource2);
        connect.map(new CoMapFunction<Sensor, String, String>() {
            @Override
            public String map1(Sensor value) throws Exception {
                return value.getId();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        }).print();
        environment.execute();
    }

    @Test
    public void unionTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        List<Sensor> list = new ArrayList<>(3);
        List<Sensor> list2 = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list2.add(Sensor.builder().id("1").temperature(101D).timestamp(4L).build());
        list2.add(Sensor.builder().id("1").temperature(99D).timestamp(2L).build());

        DataStream<Sensor> listSource = environment.fromCollection(list);
        DataStream<Sensor> listSource2 = environment.fromCollection(list2);
        listSource.union(listSource2).print();
        environment.execute();
    }

    @Test
    public void filterTransform() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        List<Sensor> list = new ArrayList<>(3);
        List<Sensor> list2 = new ArrayList<>(3);
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(1L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(2L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(3L).build());
        list2.add(Sensor.builder().id("1").temperature(101D).timestamp(4L).build());
        list2.add(Sensor.builder().id("1").temperature(99D).timestamp(2L).build());

        DataStream<Sensor> listSource = environment.fromCollection(list);
        DataStream<Sensor> listSource2 = environment.fromCollection(list2);
        listSource.union(listSource2).process(new MyFilterProcess()).filter(new MyFilterFunction()).print();
        environment.execute();
    }

}
/*万能操作*/
class MyFilterProcess extends ProcessFunction<Sensor,Sensor> {
    @Override
    public void processElement(Sensor value, Context ctx, Collector<Sensor> out) throws Exception {
        if (value.getId().equals("1")){
            out.collect(value);
        }
    }
}

class MyFilterFunction implements FilterFunction<Sensor> {
    @Override
    public boolean filter(Sensor value) throws Exception {
        return value.getTemperature()==100D;
    }
}
/*富函数有运行时上下文*/
class MyRichFilterFuntion extends RichFilterFunction<Sensor> {
    @Override
    public boolean filter(Sensor value) throws Exception {
        return value.getTemperature()==100D;
    }
}

