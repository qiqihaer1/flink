package com.nx.streaming.lesson02;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：当接收到的相同 key 的元素个数等于 3个就计算这些元素的 value 的平均值。
 * 计算keyed stream中每3个元素的 value 的平均值
 */
public class TestKeyedStateMain {
    public static void main(String[] args) throws  Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(16);
        //获取数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(2L, 2L),
                        Tuple2.of(2L, 6L));
        /**
         * 1:
         *    1,3
         *    1,7
         *    1,5
         *    15/3 = 5
         * 2:
         *   2,4
         *   2,2
         *   2,6
         *   12/3 = 4
         *   state:
         *   1. operator state
         *   2. keyed state
         */

        // 输出：
        //(1,5.0)
        //(2,4.0)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountAverageWithMapState())
                .print();


        env.execute("TestStatefulApi");
    }
}
