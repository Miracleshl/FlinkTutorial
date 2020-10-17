package com.miracle.worldcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * 流处理统计
 *
 * @author QianShuang
 */
public class StreamWordCountTest {
    private static final String BLANK = " ";

    @Test
    public void worldCount() {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getParallelism());
        //接收一个socket文本流
        DataStreamSource<String> streamSource = env.socketTextStream("192.168.137.128", 7777);
        //进行统计处理
        DataStream<Tuple2<String, Integer>> dataStream = streamSource.flatMap((String line, Collector<Tuple2<String, Integer>> collector) ->
                Arrays.stream(line.split(BLANK)).forEach(world -> collector.collect(Tuple2.of(world, 1))))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .filter(item -> StringUtils.isNotBlank(item.f0))
                .keyBy("f0")
                .sum(1);
        dataStream.print();
        try {
            env.execute("worldCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void worldCountDynamicParam() {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\flink.properties";
        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        //接收一个socket文本流
        DataStreamSource<String> streamSource = env.socketTextStream(host, port);
        //进行统计处理
        DataStream<Tuple2<String, Integer>> dataStream = streamSource.flatMap((String line, Collector<Tuple2<String, Integer>> collector) ->
                Arrays.stream(line.split(BLANK)).forEach(world -> collector.collect(Tuple2.of(world, 1))))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .filter(item -> StringUtils.isNotBlank(item.f0))
                .keyBy("f0")
                .sum(1);
        //setParallelism用来设置并发数
        dataStream.print().setParallelism(1);
        try {
            env.execute("worldCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
