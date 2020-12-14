package com.miracle.bean;

import com.esotericsoftware.kryo.DefaultSerializer;
import lombok.*;

import java.io.Serializable;

/**
 * @author QianShuang
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sensor implements Serializable {
    private String id;
    private Long timestamp;
    private Double temperature;
}
