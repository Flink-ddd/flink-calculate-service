package com.panda.flink.sink;

import com.panda.flink.business.user.User;
import com.panda.flink.server.StartFlinkKafkaUserServer;
import com.panda.flink.source.KafkaUserSourceFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author muxiaohui
 * @desc 从Kafka中消费数据写入到MySQL对应的表中
 */
public class FlinkSqlToUser {
    static Logger logger = LoggerFactory.getLogger(StartFlinkKafkaUserServer.class);

    public static void main(String[] args) throws Exception {
        System.out.println("out:开始启动FlinkSqlToUser服务");
        logger.info("开始启动FlinkSqlToUser服务");
        //无界数据流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        //每隔5000ms进行启动一个检查点
        env.enableCheckpointing(5000);
        //设置模式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有进行500 ms的进度
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //注意此处，必需设为TimeCharacteristic.EventTime，表示采用数据流元素事件时间（可以是元素时间字段、也可以自定义系统时间）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //读取kafka数据源
        KafkaUserSourceFunction kafkaUserSourceFunction = new KafkaUserSourceFunction();
        DataStream<User> source = kafkaUserSourceFunction.run(env);
        //将统计结果输出到记录表
        source.addSink(new JdbcUserWriter());
        env.execute("flink mysql to user");
    }
}
