package com.nx.streaming.lesson02;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 根据Key进行累加，实现类似单词计数的效果
 */
public class TestKeyedStateMain2 {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 6L));

        /**
         * 1,3
         * 1,7   ->  1,10
         * 1,5   -> 1,15
         *
         *
         */

        dataStreamSource
                .keyBy(0)
                .flatMap(new SumFunction())
                .print();


        env.execute("TestStatefulApi");
    }
}
