package com.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {

            @Override
            public String map(Long aLong) throws Exception {
                return "number " + aLong;
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), Types.STRING);

        env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "Source").print();
        env.execute();
    }
}
