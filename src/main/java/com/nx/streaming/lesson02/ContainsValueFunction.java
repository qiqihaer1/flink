package com.nx.streaming.lesson02;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ContainsValueFunction
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {
    private AggregatingState<Long, String> totalStr;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        AggregatingStateDescriptor<Long, String, String> descriptor =
                new AggregatingStateDescriptor<Long, String, String>(
                        "totalStr",  // 状态的名字

                        //Spark
                        new AggregateFunction<Long, String, String>() {
                            //初始化辅助变量
                            @Override
                            public String createAccumulator() {
                                return "Contains：";
                            }

                            /**
                             *
                             * @param value
                             * @param accumulator
                             * @return
                             */
                            @Override
                            public String add(Long value, String accumulator) {
                                if ("Contains：".equals(accumulator)) {
                                    //Contains: 3
                                    return accumulator + value;
                                }
                                //Contains: 3 and 5
                                return accumulator + " and " + value;
                            }

                            @Override
                            public String merge(String a, String b) {
                                return a + " and " + b;
                            }


                            @Override
                            public String getResult(String accumulator) {
                                return accumulator;
                            }
                        }, String.class); // 状态存储的数据类型
        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element,
                        Collector<Tuple2<Long, String>> out) throws Exception {
        totalStr.add(element.f1);

        out.collect(Tuple2.of(element.f0, totalStr.get()));
    }
}
