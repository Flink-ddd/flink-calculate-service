package com.panda.flink.service;

import com.panda.flink.business.user.User;
import com.panda.flink.enums.QuotaEnum;
import com.panda.flink.sink.RedisDataRichSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Description 用户统计公共算子
 * @Author muxiaohui
 * @Date 2023/06/30
 * @Version V1.0
 */
@Slf4j
public class FlinkUserService {
    static Logger logger = LoggerFactory.getLogger(FlinkUserService.class);

    /**
     * 公共聚合统计方法
     *
     * @param name       设置当前数据流名称，用于可始化和日志查看
     * @param quotaEnum  统计维度
     * @param dataStream 数据源
     * @param command    Redis指令类型k/v|k/hash
     * @param key        缓存key
     * @param append     是否追加数据（累计到之前的统计结果上）
     * @return
     */
    public static DataStream<Tuple3<String, Integer, String>> commonCount(String name, QuotaEnum quotaEnum,
                                                                          DataStream<User> dataStream,
                                                                          RedisCommand command,
                                                                          String key,
                                                                          boolean append) {
        //Tuple3中三个参数分别为用户名、注册标签（人数）、注册来源
        DataStream<Tuple3<String, Integer, String>> output = dataStream
                .map(new MapFunction<User, Tuple4<String, Integer, String, Long>>() {
                    @Override
                    public Tuple4<String, Integer, String, Long> map(User user) throws Exception {
                        String key = "default";
                        if (quotaEnum == QuotaEnum.USER) {
                            //演示以用户ID为keyBy分区字段
                            key = user.getUserId();
                        } else if (QuotaEnum.ADDRESS == quotaEnum) {
                            key = user.getAddress();
                        } else if (QuotaEnum.GENDER == quotaEnum) {
                            key = user.getGender();
                        } else if (QuotaEnum.AGE == quotaEnum) {
                            key = user.getAge();
                        }
                        //Tuple4中四个参数分别为统计维度  名称、注册标签（人数）、注册来源、用户注册时间
                        return Tuple4.of(key, user.getRegisterLabel(), user.getRegisterSource(), user.getRegisterTimeSeries());
                    }
                })
                //为一个水位线，这个Watermarks在不断的变化，一旦Watermarks大于了某个window的end_time，就会触发此window的计算，Watermarks就是用来触发window计算的。
                //Duration.ofSeconds(3)，到数据流到达flink后，再水位线中设置延迟时间，也就是在所有数据流的最大的事件时间比window窗口结束时间大或相等时，再延迟多久触发window窗口结束；
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, timestamp) -> {
                                    //System.out.println(element.f1 + ","+ element.f0 + "的水位线为：" + DateFormatUtils.format(element.f3, "yyyy-MM-dd HH:mm:ss"));
                                    return element.f3;
                                })
                )
                .keyBy((KeySelector<Tuple4<String, Integer, String, Long>, String>) k -> k.f0)
                //按半分钟为一个流窗口，但窗口从第0秒钟开始
                .window(TumblingEventTimeWindows.of(Time.seconds(30), Time.seconds(0)))
                //处理窗口事件下的所有流元素
                .process(new ProcessWindowFunction<Tuple4<String, Integer, String, Long>, Tuple3<String, Integer, String>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple4<String, Integer, String, Long>> elements, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        logger.info("计算窗口时间周期，startTime:" + DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss") + ", endTime:" + DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss"));
                        Iterator<Tuple4<String, Integer, String, Long>> iterator = elements.iterator();
                        Integer totalNum = 0;
                        String registerSource = "";
                        while (iterator.hasNext()) {
                            Tuple4<String, Integer, String, Long> t4 = iterator.next();
                            totalNum += t4.f1;
                            registerSource = t4.f2;
                        }
                        out.collect(Tuple3.of(s, totalNum, registerSource));
                    }
                })
                //使用lambda表达式，注意返回类型要和Tuple类型保持一致，否则会报错。
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
                .name(name);
//        output.print();

        //保存数据到redis中
        if (key != null) {
            output.map(new MapFunction<Tuple3<String, Integer, String>, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(Tuple3<String, Integer, String> t3) throws Exception {
                    return Tuple2.of(t3.f0, t3.f1.toString());
                }
            }).addSink(new RedisDataRichSink(key, command, append));
            output.print();
        }
        return output;
    }

}
