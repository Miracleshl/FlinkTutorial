package com.miracle.dev.workdemo;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class ReportRule implements Serializable {
    private String tag;
    private String rowDelimiter;
    private String fieldDelimiter;
}
