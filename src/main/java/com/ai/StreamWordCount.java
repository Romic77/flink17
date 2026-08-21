package com.ai;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.activation.FileDataSource;
import java.io.File;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流式执行环节
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建 FileSource
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\workspace\\flink17\\words.txt")).build();

        DataStream<String> dataStream = streamExecutionEnvironment.fromSource(fileSource, WatermarkStrategy.<String>noWatermarks(), "file-source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {

                    collector.collect(
                            Tuple2.of(word, 1)
                    );

                }
            }
        }).keyBy(t -> t.f0).sum(1);

        sum.print();

        streamExecutionEnvironment.execute();

    }

}
