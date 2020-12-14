package com.miracle.processapi;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = environment.socketTextStream("192.168.137.128", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = sourceStream.map(line -> {
            String[] vars = line.split(",");
            return new Sensor(vars[0], Long.valueOf(vars[1]), Double.valueOf(vars[2]));
        }).process(new MySplitProcessFunction(30));
        dataStream.getSideOutput(new OutputTag<>("low", Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE))).print("low");
        dataStream.print("hight");
        environment.execute();
    }
}

class MySplitProcessFunction extends ProcessFunction<Sensor, Sensor> {

    private final int threshold;

    public MySplitProcessFunction(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void processElement(Sensor value, Context ctx, Collector<Sensor> out) throws Exception {
        if (value.getTemperature() > threshold) {
            out.collect(value);
        } else {
            ctx.output(new OutputTag<>("low",Types.TUPLE(Types.STRING,Types.LONG,Types.DOUBLE)), Tuple3.of(value.getId(), value.getTimestamp(), value.getTemperature()));
        }
    }
}
