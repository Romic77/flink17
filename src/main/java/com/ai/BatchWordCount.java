package com.ai;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 *
 */
public class BatchWordCount
{
    public static void main(String[] args) throws Exception {
        //获取执行环节
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据，行读取
        DataSource<String> stringDataSource = env.readTextFile("D:\\workspace\\flink17\\words.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            //切分、转换
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //分隔
                String[] words = line.split(" ");
                for (String word : words) {
                    //向下游发送数据，将原来的行数据转换成元组 (flink,1)
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        System.out.println(stringTuple2FlatMapOperator.toString());

        //按照索引分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);

        //聚合，按照索引1来聚合
        tuple2UnsortedGrouping.sum(1).print();
    }

}
