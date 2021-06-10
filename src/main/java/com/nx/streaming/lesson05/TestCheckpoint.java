package com.nx.streaming.lesson05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TestCheckpoint {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new FsStateBackend("hdfs://192.168.123.102:9000/flink/checkpoint"));

        //设置checkpoint //建议不要太短，我个人在企业里面使用的经验，我们设置在5分钟。
        env.enableCheckpointing(5000);//5s

        //设置checkpoint支持的语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //两次checkpoint的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //最多三个checkpoints同时进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //checkpoint超时的时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));


        //cancel程序的时候保存checkpoint
        //flink cancel
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                return "nx_" + line;
            }
        }).uid("split-map");
        result.print().uid("print-operator");

        env.execute("test ");
    }
}
