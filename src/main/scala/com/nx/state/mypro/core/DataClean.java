package com.nx.state.mypro.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.nx.state.mypro.source.NxMysqlSource;
import com.nx.state.mypro.source.NxRedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 * 码表
 */
public class DataClean {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //我们是从Kafka里面读取数据，所以这儿就是topic有多少个partition，那么就设置几个并行度。
        env.setParallelism(3);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //注释，我们这儿其实需要设置state backed类型，我们要把checkpoint的数据存储到
        //rocksdb里面

        //第一步：从Kafka里面读取数据 消费者 数据源需要kafka
        //topic的取名还是有讲究的，最好就是让人根据这个名字就能知道里面有什么数据。
        //xxxx_xxx_xxx_xxx
        String KAFKA_TOPIC="from_flume01";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.server","vmnode01:9092");
        consumerProperties.put("group.id","allTopic_consumer");
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("auto.offset.reset","earliest");
//        consumerProperties.put("auto.offset.reset", "latest");
//        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        /**
         * String topic, 主题
         * KafkaDeserializationSchema<T> deserializer,
         * Properties props
         */

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                KAFKA_TOPIC,
                new SimpleStringSchema(),//序列化格式，直接使用flink已有的函数
                consumerProperties);


        /**
         * Specifies whether or not the consumer should commit offsets back to Kafka on checkpoints.
         *
         * <p>This setting will only have effect if checkpointing is enabled for the job.
         * If checkpointing isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
         * property settings will be used.
         *
         * @return The consumer object, to allow function chaining.
         */
//        consumer.setCommitOffsetsOnCheckpoints(true);

        /**
         * 获取数据源
         */
        //{"dt":"2019-11-24 19:54:23","countryCode":"PK","data":[{"type":"s4","score":0.8,"level":"C"},{"type":"s5","score":0.2,"level":"C"}]}
        DataStreamSource<String> allData = env.addSource(kafkaConsumer);
//        DataStreamSource<HashMap<String, String>> mapData = env.addSource(new NxRedisSource());
//        DataStream<HashMap<String, String>> mapData = env.addSource(new NxRedisSource()).broadcast();
        DataStream<HashMap<String, String>> mapData = env.addSource(new NxMysqlSource()).broadcast();

        /**
         * ETL处理
         */
        SingleOutputStreamOperator<String> etlData = allData.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {


            HashMap<String, String> allMap = new HashMap<String, String>();

            //里面处理的是kafka的数据
            @Override
            public void flatMap1(String line, Collector<String> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(line);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                //可以根据countryCode获取大区的名字
                String area = allMap.get(countryCode);
                JSONArray data = jsonObject.getJSONArray("data");
                for (int i = 0; i < data.size(); i++) {
                    JSONObject dataObject = data.getJSONObject(i);
                    System.out.println("大区："+area);
                    dataObject.put("dt", dt);
                    dataObject.put("area", area);
                    //下游获取到数据的时候，也就是一个json格式的数据
                    out.collect(dataObject.toJSONString());
                }


            }

            //里面处理的是redis里面的数据
            @Override
            public void flatMap2(HashMap<String, String> map,
                                 Collector<String> collector) throws Exception {
                System.out.println(map.toString());
                //将redis的map数据存入allMap
                allMap = map;

            }
        });




        String outputTopic="allDataClean";
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers","192.168.200.101:9092");
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outputTopic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                producerProperties);

        //搞一个Kafka的生产者
        etlData.addSink(producer);

        env.execute("DataClean");

    }

}
