package com.miracle.processapi;

import com.miracle.bean.Sensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public class ProcessApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = environment.socketTextStream("192.168.137.128",7777);
        sourceStream.map( line -> {
            String[] vars = line.split(",");
            return new Sensor(vars[0],Long.valueOf(vars[1]),Double.valueOf(vars[2]));
        }).keyBy(sensor -> sensor.getId()).process(new TempIncrementWarning(10000)).print("warning");
        environment.execute();
    }
}
class MyKeyedProcessFunction extends KeyedProcessFunction<String,Sensor,String> {
    private ValueState<Double> state ;
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void processElement(Sensor value, Context ctx, Collector<String> out) throws Exception {
        ctx.getCurrentKey();
        ctx.timerService().currentWatermark();
        ctx.timerService().registerEventTimeTimer(value.getTimestamp());
        ctx.timerService().deleteEventTimeTimer(value.getTimestamp());
        ctx.output(new OutputTag<Sensor>("side"),value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("last",Double.class));
    }
}
class TempIncrementWarning extends KeyedProcessFunction<String,Sensor,String> {

    private ValueState<Double> last ;
    private ValueState<Long> timer;
    private Long diff;

    public TempIncrementWarning (long diff){
        this.diff = diff;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        last = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp",Double.class));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("registerTimer",Long.class));
    }

    @Override
    public void processElement(Sensor value, Context ctx, Collector<String> out) throws Exception {
        if (Objects.isNull(last.value())){
            timer.update(ctx.timerService().currentProcessingTime()+diff);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+diff);
        }else {
            if (Double.compare(value.getTemperature(),last.value())<=0){
                ctx.timerService().deleteProcessingTimeTimer(timer.value());
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+diff);
                timer.update(ctx.timerService().currentProcessingTime()+diff);
            }
        }
        last.update(value.getTemperature());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        out.collect("连续"+diff/1000+"秒"+ctx.getCurrentKey()+"温度递增或不变;");
        ctx.timerService().registerProcessingTimeTimer(timestamp+diff);
        timer.update(timestamp+diff);
    }
}