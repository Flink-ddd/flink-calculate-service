package com.panda.flink.server;

import com.panda.flink.business.user.User;
import com.panda.flink.enums.QuotaEnum;
import com.panda.flink.service.FlinkUserService;
import com.panda.flink.sink.JdbcUserWriter;
import com.panda.flink.source.KafkaUserSourceFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author muxiaohui
 * @Description Flink数据流实时计算示例：启动对接kafka数据，流处理flink业务服务job，模拟对用户注册信息多维度聚合统计;
 */
@Slf4j
public class StartFlinkKafkaUserServer {
    static Logger logger = LoggerFactory.getLogger(StartFlinkKafkaUserServer.class);
    /**
     * 窗口事件时间
     */
    static final int EVENT_TIME = 1;

    /**
     * 主进程方法
     *
     * @throws Exception
     */
    public static void main(String[] args) {
        System.out.println("out:开始启动StartFlinkKafkaUserServer服务");
        logger.info("开始启动StartFlinkKafkaUserServer服务");
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
        //用户注册数据写入MySQL
//        sendUserDataToMysql(source);
        //统计注册用户数
        countRegisterUserNum(source);
        //按性别统计注册用户数
        countGenderRegisterUserNum(source);
        //按地区统计注册用户数
        countAddressRegisterUserNum(source);
        //按年龄统计用户注册数
        countAgeRegisterUserNum(source);
        System.out.println("out:执行job任务");
        logger.info("执行job任务");
        //执行JOB
        try {
            env.execute("用户聚合统计JOB");
        } catch (Exception e) {
            logger.error("用户聚合统计JOB,执行异常！", e);
        }
    }

    private static void sendUserDataToMysql(DataStream<User> source) {
        source.addSink(new JdbcUserWriter());
        log.info("读取flink算子中的用户注册数据写入MySQL成功");
    }

    /**
     * 统计注册用户数
     *
     * @param userDataStream
     */
    private static void countRegisterUserNum(DataStream<User> userDataStream) {
        FlinkUserService.commonCount("countRegisterUserNum", QuotaEnum.DEFAULT, userDataStream, RedisCommand.HSET, "FLINK_USER_REGISTER_NUM", true);
    }

    /**
     * 按性别统计注册用户数
     *
     * @param userDataStream
     */
    private static void countGenderRegisterUserNum(DataStream<User> userDataStream) {
        FlinkUserService.commonCount("countGenderRegisterUserNum", QuotaEnum.GENDER, userDataStream, RedisCommand.HSET, "FLINK_USER_GENDER_REGISTER_NUM", true);
    }

    /**
     * 按地区统计注册用户数
     *
     * @param userDataStream
     */
    private static void countAddressRegisterUserNum(DataStream<User> userDataStream) {
        FlinkUserService.commonCount("countAddressRegisterUserNum", QuotaEnum.ADDRESS, userDataStream, RedisCommand.HSET, "FLINK_USER_ADDRESS_REGISTER_NUM", true);
    }

    /**
     * 按年龄统计用户注册数
     *
     * @param userDataStream
     */
    private static void countAgeRegisterUserNum(DataStream<User> userDataStream) {
        FlinkUserService.commonCount("countAgeRegisterUserNum", QuotaEnum.AGE, userDataStream, RedisCommand.HSET, "FLINK_USER_AGE_REGISTER_NUM", true);
    }


}
