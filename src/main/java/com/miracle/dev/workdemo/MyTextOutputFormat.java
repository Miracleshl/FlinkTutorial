package com.miracle.dev.workdemo;

import com.miracle.bean.FieldOrder;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class MyTextOutputFormat<T> extends FileOutputFormat<T> {
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputFormat.class);

    // --------------------------------------------------------------------------------------------

    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String DEFAULT_FIELD_DELIMITER = ",";

    // --------------------------------------------------------------------------------------------

    private transient Writer wrt;

    private String fieldDelimiter;

    private String recordDelimiter;

    private String charsetName;

    private boolean allowNullValues = true;

    private boolean quoteStrings = false;

    private List<Field> fields;

    // --------------------------------------------------------------------------------------------
    // Constructors and getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    /**
     * Creates an instance of CsvOutputFormat. Lines are separated by the newline character '\n',
     * fields are separated by ','.
     *
     * @param outputPath The path where the CSV file is written.
     */
    public MyTextOutputFormat(Path outputPath) {
        this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
    }

    /**
     * Creates an instance of CsvOutputFormat. Lines are separated by the newline character '\n',
     * fields by the given field delimiter.
     *
     * @param outputPath     The path where the CSV file is written.
     * @param fieldDelimiter The delimiter that is used to separate fields in a tuple.
     */
    public MyTextOutputFormat(Path outputPath, String fieldDelimiter) {
        this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
    }

    /**
     * Creates an instance of CsvOutputFormat.
     *
     * @param outputPath      The path where the CSV file is written.
     * @param recordDelimiter The delimiter that is used to separate the tuples.
     * @param fieldDelimiter  The delimiter that is used to separate fields in a tuple.
     */
    public MyTextOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
        super(outputPath);
        if (recordDelimiter == null) {
            throw new IllegalArgumentException("RecordDelmiter shall not be null.");
        }

        if (fieldDelimiter == null) {
            throw new IllegalArgumentException("FieldDelimiter shall not be null.");
        }

        this.fieldDelimiter = fieldDelimiter;
        this.recordDelimiter = recordDelimiter;
        this.allowNullValues = false;
    }

    /**
     * Configures the format to either allow null values (writing an empty field),
     * or to throw an exception when encountering a null field.
     *
     * <p>by default, null values are disallowed.
     *
     * @param allowNulls Flag to indicate whether the output format should accept null values.
     */
    public void setAllowNullValues(boolean allowNulls) {
        this.allowNullValues = allowNulls;
    }

    /**
     * Sets the charset with which the CSV strings are written to the file.
     * If not specified, the output format uses the systems default character encoding.
     *
     * @param charsetName The name of charset to use for encoding the output.
     */
    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    /**
     * Configures whether the output format should quote string values. String values are fields
     * of type {@link java.lang.String} and {@link org.apache.flink.types.StringValue}, as well as
     * all subclasses of the latter.
     *
     * <p>By default, strings are not quoted.
     *
     * @param quoteStrings Flag indicating whether string fields should be quoted.
     */
    public void setQuoteStrings(boolean quoteStrings) {
        this.quoteStrings = quoteStrings;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
                new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.flush();
            this.wrt.close();
        }
        super.close();
    }

    @Override
    public void writeRecord(T element) throws IOException {
        if (element instanceof String ) {
            this.wrt.write(element.toString());
        }else {
            if (fields == null) {
                fields = orderField(element.getClass().getDeclaredFields());
            }
            try {
                for (int i = 0; i < fields.size(); i++) {
                    Field field = fields.get(i);
                    field.setAccessible(true);
                    Object v = field.get(element);
                    if (v != null) {
                        if (i != 0) {
                            this.wrt.write(this.fieldDelimiter);
                        }

                        if (quoteStrings) {
                            if (v instanceof String || v instanceof StringValue) {
                                this.wrt.write('"');
                                this.wrt.write(field.getName() + ":" + v.toString());
                                this.wrt.write('"');
                            } else {
                                this.wrt.write(field.getName() + ":" + v.toString());
                            }
                        } else {
                            this.wrt.write(field.getName() + ":" + v.toString());
                        }
                    } else {
                        if (this.allowNullValues) {
                            if (i != 0) {
                                this.wrt.write(this.fieldDelimiter);
                            }
                        } else {
                            throw new RuntimeException("Cannot write element with <null> value at position: " + i);
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        // add the record delimiter
        this.wrt.write(this.recordDelimiter);
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "MyTextOutputFormat (path: " + this.getOutputFilePath() + ", delimiter: " + this.fieldDelimiter + ")";
    }

    private List<Field> orderField(Field[] fields) {
        // 用来存放所有的属性域
        List<Field> fieldList = new ArrayList<>(fields.length);
        // 过滤带有注解的Field
        for (Field f : fields) {
            if (Objects.nonNull(f.getAnnotation(FieldOrder.class))) {
                fieldList.add(f);
            }
        }
        // 这个比较排序的语法依赖于java 1.8
        fieldList.sort(Comparator.comparingInt(
                m -> m.getAnnotation(FieldOrder.class).value()
        ));
        return fieldList;
    }
}