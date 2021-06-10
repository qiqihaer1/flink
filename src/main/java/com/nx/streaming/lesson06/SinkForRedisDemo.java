package com.nx.streaming.lesson06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * 从Kafka里面读取数据，然后把数据结果写入redis
 */
public class SinkForRedisDemo {
    public static void main(String[] args) throws  Exception {
        //步骤一 程序入口并且设置checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop05:9000/checkpoint"));

        //步骤二：从kafka获取数据
        String topic="test";
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers","bigdata05:9092");
        consumerProperties.setProperty("group.id","testSlot_consumer");
        consumerProperties.setProperty("auto.offset.reset","earliest");
       //把这个参数设置为false，这个参数的意思会 周期性的保存偏移量。
        consumerProperties.setProperty("enable.auto.commit"," false");

        FlinkKafkaConsumer011<String> myConsumer =
                new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), consumerProperties);



        //是否把offset写入到__consumer_offset
        // myConsumer.setCommitOffsetsOnCheckpoints(true);
        //operator state
        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(4);


        DataStream<Tuple2<String, String>> l_wordsData = data.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                //k v
                return new Tuple2<>("f", value);
            }
        });

        //步骤三：把数据写入Redis
        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.167.254").setPort(6379).build();
       //创建redis sink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        l_wordsData.addSink(redisSink);
        env.execute("StreamingDemoToRedis");

    }

    /**
     * 把数据插入到redis到逻辑
     */
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}
