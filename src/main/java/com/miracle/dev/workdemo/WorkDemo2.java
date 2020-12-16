package com.miracle.dev.workdemo;

import com.miracle.bean.Sensor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

public class WorkDemo2 {
    @Test
    public void pojoBean() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Sensor> sourceAll = environment.addSource(new MySource());
//        List<ReportRule> tagCollection = new MyTagSource();
        DataStream<ReportRule> tagCollection = null;
        //输出所有信息
        sourceAll.addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\86.txt", FileSystem.WriteMode.OVERWRITE, "\n", "|")).setParallelism(1);
        //伪分流
        SplitStream<Sensor> split = sourceAll.split(sensor -> Collections.singleton(sensor.getId()));
        //根据tag集合创建文件
//        tagCollection
//                .flatMap(value ->
//                    //输出当前tag对应的信息
//                    split.select(value.getTag())
//                            .addSink(writeUsingOutputFormat("D:\\flinkSink\\" + LocalDate.now().toString() + "\\" + value.getTag() + ".txt", FileSystem.WriteMode.OVERWRITE, value.getRowDelimiter(), value.getFieldDelimiter()))
//                            .setParallelism(1)
//                );
        environment.execute("report txt");
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
