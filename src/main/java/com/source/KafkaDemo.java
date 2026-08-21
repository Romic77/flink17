package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source= KafkaSource.<String>builder().setBootstrapServers("localhost:9092")
                .setGroupId("group1").setTopics("topic1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest()).build();

        //创建topic
        //.\bin\windows\kafka-topics.bat --create ` --topic topic1 ` --bootstrap-server localhost:9092 ` --partitions 1 ` --replication-factor 1
        //
        //producer 连接topic，然后发送消息
        //.\bin\windows\kafka-console-producer.bat ` --topic topic1 ` --bootstrap-server localhost:9092

        DataStreamSource<String> stringDataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaDemo");

        stringDataStreamSource.print();
        env.execute();
    }
}
