package com.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8080);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        DataStream<String> dataStream = env.socketTextStream("192.168.11.46", 7777);

        //可以设置 flink的模式，跑批和流计算
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String str : line.split(" ")) {
                            collector.collect(Tuple2.of(str, 1));
                        }
                    }
                    //returns是告诉返回的结果的类型是元组Tuple<String,Int>

                }).setParallelism(2)

                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t -> t.f0).sum(1);

        sum.print();

        env.execute() ;
    }
}
