package com.miracle.dev.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Arrays;

public class WordCount {
    public static String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";

    @Test
    public void batchWordCount() throws Exception {
        //创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件内容
        DataSet<String> dataStream = environment.readTextFile(path);
        DataSet<Tuple2<String, Integer>> sinkStream = dataStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) ->
                        Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1);
        sinkStream.print();
    }

    @Test
    public void streamWordCount() throws Exception {
        //创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStream<String> dataStream = environment.socketTextStream("192.168.137.128", 7777);
        DataStream<Tuple2<String, Integer>> sinkStream = dataStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) ->
                        Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        sinkStream.print();
        environment.execute("Stream Word Count");
    }
}
