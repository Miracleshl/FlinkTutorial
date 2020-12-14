package com.miracle.dev.sink;

import com.miracle.bean.Sensor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Paths;

public class SinkTest {
    @Test
    public void fileSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String,String,String>> sinkStream = fileSource.map(line->{
            String[] fields = line.split(",");
            return Tuple3.of(fields[0],fields[1],fields[2]);
        }, Types.TUPLE(Types.STRING,Types.STRING,Types.STRING));
        sinkStream.print();
        sinkStream.writeAsCsv("D:\\fileSink\\output.txt", FileSystem.WriteMode.OVERWRITE,"\n",",")
                .setParallelism(1);
        sinkStream.writeAsText("D:\\fileSink\\output.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
        environment.execute("");
    }

    @Test
    public void defineFileSink() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "E:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        DataStream<String> fileSource = environment.readTextFile(path);
        DataStream<Tuple3<String,String,String>> sinkStream = fileSource.map(line->{
            String[] fields = line.split(",");
            return Tuple3.of(fields[0],fields[1],fields[2]);
        }, Types.TUPLE(Types.STRING,Types.STRING,Types.STRING));
        sinkStream.print();
        OutputFileConfig fileConfig = new OutputFileConfig("out","txt");
        sinkStream.addSink(StreamingFileSink.forRowFormat(new Path("D:\\fileSink"), new SimpleStringEncoder<Tuple3<String,String,String>>()).withOutputFileConfig(fileConfig).build()).setParallelism(1);
        environment.execute("");
    }
}

