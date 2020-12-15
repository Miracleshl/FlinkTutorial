package com.miracle.dev.parallelism;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Arrays;

public class WordCount {
    public static String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";

    @Test
    public void streamWordCount() throws Exception {
        //创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //禁止操作合并
        environment.disableOperatorChaining();
        environment.setParallelism(2);
        DataStream<String> dataStream = environment.socketTextStream("192.168.137.128", 7777);
        DataStream<Tuple2<String, Integer>> sinkStream = dataStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) ->
                        Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1))))
                //当前步骤禁止合并到调用链中 前后断开
//                .disableChaining()
                //开启一个新的调用链 前面断开 后面可以合并
//                .startNewChain()
                //slot共享组名称 只有同名的共享组才能共享slot 默认的组名是default
//                .slotSharingGroup("")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        sinkStream.print();
        environment.execute("Stream Word Count");
    }
}
