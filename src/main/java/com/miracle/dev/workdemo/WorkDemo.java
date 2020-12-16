package com.miracle.dev.workdemo;

import com.miracle.bean.Sensor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkDemo {
    @Test
    public void pojoBean() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Set<String> tagCollection = getTagCollection();
        DataStream<Sensor> sourceAll = environment.addSource(new MySource());
        //输出所有信息
        sourceAll.addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\86.txt", FileSystem.WriteMode.OVERWRITE, "\n", "|")).setParallelism(1);
        //伪分流
        SplitStream<Sensor> split = sourceAll.split(sensor -> Collections.singleton(sensor.getId()));
        //根据tag集合创建文件
        tagCollection.stream().forEach(tag ->
                //输出当前tag对应的信息
                split.select(tag)
                        .addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\" + tag + ".txt", FileSystem.WriteMode.OVERWRITE, "\n", "|"))
                        .setParallelism(1)
        );
        environment.execute("report txt");
    }

    @Test
    public void String() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Set<String> tagCollection = getTagCollection();
        DataStream<Sensor> sourceAll = environment.addSource(new MySource());
        //输出所有信息
        sourceAll.addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\86.txt", FileSystem.WriteMode.OVERWRITE, "\n", "|")).setParallelism(1);
        //伪分流
        SplitStream<Sensor> split = sourceAll.split(sensor -> Collections.singleton(sensor.getId()));
        //根据tag集合创建文件
        tagCollection.stream().forEach(tag ->
                //输出当前tag对应的信息
                split.select(tag).map(sensor->
                        "id:"+sensor.getId()+",timestamp:"+sensor.getTimestamp()+",temperature:"+sensor.getTemperature()
                )
                        .addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\" + tag + ".txt", FileSystem.WriteMode.OVERWRITE, "\n", "|"))
                        .setParallelism(1)
        );
        environment.execute("report txt");
    }

    private static Set<String> getTagCollection() {
        List<Sensor> list = new ArrayList<>();
        list.add(Sensor.builder().id("1").temperature(100D).timestamp(100L).build());
        list.add(Sensor.builder().id("2").temperature(100D).timestamp(100L).build());
        list.add(Sensor.builder().id("3").temperature(100D).timestamp(100L).build());
        list.add(Sensor.builder().id("0").temperature(100D).timestamp(100L).build());
        Set<String> tagCollection = list.parallelStream().map(sensor -> sensor.getId()).collect(Collectors.toSet());
        return tagCollection;
    }

    public static <T> SinkFunction<T> writeUsingOutputFormat(String path,
                                                             FileSystem.WriteMode writeMode,
                                                             String rowDelimiter,
                                                             String fieldDelimiter) {
        MyTextOutputFormat<T> of = new MyTextOutputFormat(
                new Path(path),
                rowDelimiter,
                fieldDelimiter);
        if (writeMode != null) {
            of.setWriteMode(writeMode);
        }
        of.setCharsetName("UTF-8");
        return new OutputFormatSinkFunction<>(of);
    }

}
