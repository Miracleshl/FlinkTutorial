package com.miracle.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author QianShuang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sensor implements Serializable {
    @FieldOrder(3)
    private String id;
    @FieldOrder(2)
    private Long timestamp;
    @FieldOrder(1)
    private Double temperature;
}
