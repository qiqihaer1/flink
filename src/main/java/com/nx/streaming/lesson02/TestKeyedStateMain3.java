package com.nx.streaming.lesson02;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TestKeyedStateMain3 {
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
         * 结果输出：
         * 1,3
         * 1，7
         * 1, 5
         *
         * 1,Contains：3
         * 1，Conains: 3 and 7
         * 1, Contains: 3 and 7 and 5
         *
         *
         * 1,Contains：3 and 7 and 5
         * 2,Contains：4 and 2 and 6
         *
         */

        dataStreamSource
                .keyBy(0)
                .flatMap(new ContainsValueFunction())
                .print();


        env.execute("TestStatefulApi");
    }
}
