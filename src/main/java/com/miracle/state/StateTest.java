package com.miracle.state;

import com.miracle.sourceapi.Sensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Objects;

public class StateTest {
    @Test
    public void stateTest() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<String> dataStream = environment.socketTextStream("192.168.137.128",7777);
        dataStream.map((line)->{
            String[] strings = line.split(",");
            Sensor sensor = new Sensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            return sensor;
        }).keyBy("id").map(new MyRichMapper());
        environment.execute();
    }
}

//keyed State 必须用RichFunction 应为需要获取运行时上下文
class MyRichMapper extends RichMapFunction<Sensor, String> {

    private ValueState<Integer> sumState;
    private ListState<String> listState;
    private MapState<String,Integer> mapState;
    private ReducingState<Sensor> reducingState;

    @Override
    public String map(Sensor value) throws Exception {
        if (Objects.isNull(sumState.value())){
            sumState.update(1);
        }else {
            sumState.update(sumState.value()+1);
        }
        listState.add(value.getId());
        reducingState.add(value);
        mapState.put(value.getId(),sumState.value());
        System.out.println(">>>>>>>>>>>>>>"+sumState.value()+"<<<<<<<<<<<<<");
        System.out.println(sumState.value());
        listState.get().forEach(System.out::print);
        System.out.println();
        mapState.iterator().forEachRemaining(System.out::print);
        System.out.println();
        System.out.println(reducingState.get().getTemperature());
        System.out.println(">>>>>>>>>>>>>>"+sumState.value()+"<<<<<<<<<<<<<");

        return value.getId();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("value_state", Integer.class));
        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list_state",String.class));
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map_state",String.class,Integer.class));
        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reducing_state",(curData,newData)->{
            curData.setTimestamp(newData.getTimestamp());
            return curData;
        },Sensor.class));
        super.open(parameters);
    }
}
