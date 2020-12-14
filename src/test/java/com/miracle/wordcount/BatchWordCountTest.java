package com.miracle.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author QianShuang
 */
public class BatchWordCountTest {
    private static final String BLANK = " ";

    @Test
    public void worldCount() throws Exception {
        //参考 https://www.cnblogs.com/ShadowFiend/p/11951948.html

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 加载或创建源数据
        DataSet<String> text = env.fromElements("this a book", "i love china", "i am chinese");

        // 转化处理数据
        DataSet<Tuple2<String, Integer>> ds = text.flatMap(new LineSplitter()).groupBy(0).sum(1);

        // 输出数据到目的端
        ds.print();

        // 执行任务操作
        // 由于是Batch操作，当DataSet调用print方法时，源码内部已经调用Execute方法，所以此处不再调用，如果调用会出现错误
        //env.execute("Flink Batch Word Count By Java");

    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            for (String word : line.split(BLANK)) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

    @Test
    public void worldCountLambda() throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 加载或创建源数据
        String path = "D:\\bigData\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> dataSource = env.readTextFile(path);

        // 转化处理数据 输出数据到目的端
        dataSource.flatMap((String line, Collector<Tuple2<String, Integer>> collector) ->
                Arrays.stream(line.split(BLANK)).forEach(world -> collector.collect(Tuple2.of(world, 1))))
                //参考 https://blog.csdn.net/fu_huo_1993/article/details/103108847
                //因为泛型擦除所以需要在这里指定返回类型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy("f0")
                .sum(1).print();
    }
}
