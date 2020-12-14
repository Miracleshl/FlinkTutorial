package com.miracle.state;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Objects;

public class StateTest {
    @Test
    public void stateTest() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> dataStream = environment.socketTextStream("192.168.137.128", 7777);
        dataStream.map((line) -> {
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        }).keyBy(value -> value.getId()).map(new MyRichMapper());
        environment.execute();
    }

    @Test
    public void stateTest1() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> sourceStream = environment.socketTextStream("192.168.137.128", 7777);
        sourceStream.map(line -> {
            String[] vars = line.split(",");
            return new Sensor(vars[0], Long.valueOf(vars[1]), Double.valueOf(vars[2]));
        })
                .keyBy(sensor -> sensor.getId())
                .flatMap(new MyFlatMapFunction(10)).print("print>>>");
        environment.execute();
    }
    @Test
    public void stateTest2() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> sourceStream = environment.socketTextStream("192.168.137.128", 7777);
        sourceStream.map(line -> {
            String[] vars = line.split(",");
            return new Sensor(vars[0], Long.valueOf(vars[1]), Double.valueOf(vars[2]));
        })
                .keyBy(sensor -> sensor.getId())
                .flatMap(new MyFlatMapFunction(10)).print("print>>>");
        environment.execute();
    }
}

class MyFlatMapFunction extends RichFlatMapFunction<Sensor, String> {
    private double difference;
    private ValueState<Double> last;

    public MyFlatMapFunction(double difference) {
        this.difference = difference;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        last = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Double.class));
    }

    @Override
    public void flatMap(Sensor value, Collector<String> out) throws Exception {
        if (Objects.isNull(last.value())) {
            last.update(value.getTemperature());
        }
        if (Math.abs(value.getTemperature() - last.value()) > difference) {
            out.collect(value.getId() + "温度跳变");
        }
        last.update(value.getTemperature());
    }
}

//keyed State 必须用RichFunction 应为需要获取运行时上下文
class MyRichMapper extends RichMapFunction<Sensor, String> {

    private ValueState<Integer> sumState;
    private ListState<String> listState;
    private MapState<String, Integer> mapState;
    private ReducingState<Sensor> reducingState;

    @Override
    public String map(Sensor value) throws Exception {
        if (Objects.isNull(sumState.value())) {
            sumState.update(1);
        } else {
            sumState.update(sumState.value() + 1);
        }
        listState.add(value.getId());
        reducingState.add(value);
        mapState.put(value.getId(), sumState.value());
        System.out.println(">>>>>>>>>>>>>>" + sumState.value() + "<<<<<<<<<<<<<");
        System.out.println(sumState.value());
        listState.get().forEach(System.out::print);
        System.out.println();
        mapState.iterator().forEachRemaining(System.out::print);
        System.out.println();
        System.out.println(reducingState.get().getTemperature());
        System.out.println(">>>>>>>>>>>>>>" + sumState.value() + "<<<<<<<<<<<<<");

        return value.getId();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("value_state", Integer.class));
        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list_state", String.class));
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map_state", String.class, Integer.class));
        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reducing_state", (curData, newData) -> {
            curData.setTimestamp(newData.getTimestamp());
            return curData;
        }, Sensor.class));
        super.open(parameters);
    }
}
