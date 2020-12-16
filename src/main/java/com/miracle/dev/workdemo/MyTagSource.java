package com.miracle.dev.workdemo;

import com.miracle.bean.Sensor;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.*;
import java.util.stream.Collectors;
@NoArgsConstructor
public class MyTagSource implements SourceFunction<ReportRule> {

    @Override
    public void run(SourceContext<ReportRule> ctx) throws Exception {
        List<ReportRule> list = new ArrayList<>();
        list.add(ReportRule.builder().tag("1").rowDelimiter("\n").fieldDelimiter(",").build());
        list.add(ReportRule.builder().tag("2").rowDelimiter("\n").fieldDelimiter(",").build());
        list.add(ReportRule.builder().tag("3").rowDelimiter("\n").fieldDelimiter(",").build());
        list.add(ReportRule.builder().tag("0").rowDelimiter("\n").fieldDelimiter(",").build());
        Thread.sleep(6000L);
        list.stream().forEach(tag->ctx.collect(tag));
    }

    @Override
    public void cancel() {

    }

}

