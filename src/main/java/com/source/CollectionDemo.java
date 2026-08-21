package com.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment 和 ExecutionEnvironment 是2个东西
        //StreamExecutionEnvironment 流处理，一定需要 env.execute();
        //ExecutionEnvironment 是批处理，不需要env.execute();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从集合获取类型
        //DataSource<Integer> integerDataSource = env.fromCollection(Arrays.asList(1, 2, 3));
        DataStream<Integer> integerDataSource = env.fromElements(1, 2, 3, 4, 5);

        integerDataSource.print();

        env.execute();
    }


//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<Integer> integerDataSource = env.fromElements(1, 2, 3, 4, 5);
//        integerDataSource.print();
//    }
}
