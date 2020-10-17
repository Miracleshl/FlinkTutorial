package com.miracle.sourceapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author QianShuang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Sensor {
    private String id;
    private Long timestamp;
    private Double temperature;
}
