package com.nx.streaming.lesson01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词计数
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        //TODO 步骤一：初始化程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //TODO 设置任务的并行度
        env.setParallelism(2);
        //步骤二：数据的输入
        //socket支持的并行度就是1
        String hostname="192.168.123.102";
        int port = 9999;
        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port); //1 task
        //步骤三：数据的处理
        SingleOutputStreamOperator<WordAndCount> result = dataStream
                .flatMap(new SplitWordFunction())  //2 task
                .keyBy("word")
                .sum("count");  //2 task  subtask state
        //步骤四：数据的输出
        result.print(); //2 task  sink
        //步骤五：启动程序
        env.execute("test word count...");
        /**
         * 这个任务启动了以后，会有几个task任务？
         * 10 task
         * 1. socketTextStream 操作，  1
         * 2. (keyBy+sum)是一个完整的操作
         *
         * 7 task
         */
    }


    /**
     * 分割单词
     */
    public static class SplitWordFunction implements FlatMapFunction<String,WordAndCount>{
        @Override
        public void flatMap(String line, Collector<WordAndCount> out) throws Exception {
            String[] fields = line.split(",");
            for (String word : fields) {
                out.collect(new WordAndCount(word, 1));
            }
        }
    }


    public static class WordAndCount{
        private String word;
        private Integer count;
        public WordAndCount(){

        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public WordAndCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }
}
