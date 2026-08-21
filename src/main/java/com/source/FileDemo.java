package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        FileSource<String> stringFileSourceBuilder = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                new Path("D:\\workspace\\flink17\\words.txt")).build();

        env.fromSource(stringFileSourceBuilder, WatermarkStrategy.noWatermarks(), "fileDemo").print();

        env.execute("File Demo");
    }
}
