package com.miracle.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;
import scala.Tuple2$mcII$sp;

import java.util.Arrays;

public class CacheWordCount {
    @Test
    public void temp() {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> streamSource = env.readTextFile("E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt");
        DataStream<Tuple2<String,Integer>> sinkStream = streamSource.flatMap((String line, Collector<String> collector) ->
                Arrays.stream(line.split(" ")).forEach(word -> collector.collect(word))
        ).map(word->Tuple2.of(word,1)).keyBy(word->word).sum(1);

    }
}
