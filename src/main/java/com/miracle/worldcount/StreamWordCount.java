package com.miracle.worldcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * 流处理统计
 *
 * @author QianShuang
 */
public class StreamWordCount {
    private static final String BLANK = " ";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DEFAULT_HOST = "192.168.137.128";
    private static final Integer DEFAULT_PORT = 7777;

    public static void main(String[] args) {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "flink.properties";
        ParameterTool parameterTool;
        String host;
        Integer port;
        try {
            parameterTool = ParameterTool.fromPropertiesFile(path);
        } catch (IOException e) {
            parameterTool = ParameterTool.fromArgs(args);
        }
        host = StringUtils.isBlank(parameterTool.get(HOST)) ? DEFAULT_HOST : parameterTool.get(HOST);
        port = Objects.isNull(parameterTool.getInt(PORT)) ? DEFAULT_PORT : parameterTool.getInt(PORT);
        //接收一个socket文本流
        DataStreamSource<String> streamSource = env.socketTextStream(host, port);
        //进行统计处理
        DataStream<Tuple2<String, Integer>> dataStream = streamSource.flatMap((String line, Collector<Tuple2<String, Integer>> collector) ->
                Arrays.stream(line.split(BLANK)).forEach(world -> collector.collect(Tuple2.of(world, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .filter(item -> StringUtils.isNotBlank(item.f0))
                .keyBy("f0")
                .sum(1);
        //setParallelism用来设置并发数
        dataStream.print().setParallelism(1);
        try {
            env.execute("stream world count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void wordCount() {
        //创建运行时环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> dataSet = env.readTextFile(path);
        //对数据进行转换处理统计 分割 分组 统计
        DataSet<Tuple2<String, Integer>> result = dataSet.flatMap((FlatMapFunction<String, String>) (line, collect) -> {
            String[] strings = line.split(BLANK);
            Arrays.stream(strings).forEach(world -> collect.collect(world));
        })
                .returns(Types.STRING)
                .map(world -> Tuple2.of(world, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy("f0")
                .sum(1);
        try {
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void wordCount2() {
        //创建运行时环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> dataSet = env.readTextFile(path);
        //对数据进行转换处理统计 分割 分组 统计
        DataSet<Tuple2<String, Integer>> result = dataSet.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collect) -> {
            String[] strings = line.split(BLANK);
            Arrays.stream(strings).forEach(world -> collect.collect(Tuple2.of(world, 1)));
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy("f0")
                .sum(1);
        try {
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void wordCount3() {
        //创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //从文件中读取数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStream<String> dataSet = env.readTextFile(path);
        //对数据进行转换处理统计 分割 分组 统计
        DataStream<Tuple2<String, Integer>> result = dataSet.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collect) -> {
            String[] strings = line.split(BLANK);
            Arrays.stream(strings).forEach(world -> collect.collect(Tuple2.of(world, 1)));
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((in) -> in.getField(0))
                .sum(1);
        try {
            result.print();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void wordCount4() {
        //创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //从文件中读取数据
//        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStream<String> dataSet = env.socketTextStream(DEFAULT_HOST,DEFAULT_PORT);
        //对数据进行转换处理统计 分割 分组 统计
        DataStream<Tuple2<String, Integer>> result = dataSet.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collect) -> {
            String[] strings = line.split(BLANK);
            Arrays.stream(strings).forEach(world -> collect.collect(Tuple2.of(world, 1)));
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((in) -> in.getField(0))
                .sum(1);
        try {
            result.print("print");
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
