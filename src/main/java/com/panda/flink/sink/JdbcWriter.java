package com.panda.flink.sink;


import com.panda.flink.business.order.Order;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author muxiaohui
 * @Description 将Flink的流中数据写入到MySQL数据源中
 */
public class JdbcWriter extends RichSinkFunction<Order> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    /**
     * job开始执行，调用此方法创建jdbc数据源连接对象
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(MysqlConfig.DRIVER_CLASS);
        // 获取数据库连接
        connection = DriverManager.getConnection(MysqlConfig.SOURCE_DRIVER_URL, MysqlConfig.SOURCE_USER, MysqlConfig.SOURCE_PASSWORD);
        preparedStatement =
                connection.prepareStatement("insert into charts_order_info(order_id, user_name,gender,goods,goods_type,brand,order_time,order_time_series,price,num,total_price,status,address,phone) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    }

    /**
     * job执行完毕后，调用此方法关闭jdbc连接源
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 此方法实现接口中的invoke，在DataStream数据流中的每一条记录均会调用本方法执行数据同步
     *
     * @param order
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Order order, Context context) throws Exception {
        try {
            //获取JdbcReader发送过来的结果
            preparedStatement.setString(1, order.getOrderId());
            preparedStatement.setString(2, order.getUserName());
            preparedStatement.setString(3, order.getGender());
            preparedStatement.setString(4, order.getGoods());
            preparedStatement.setString(5, order.getGoodsType());
            preparedStatement.setString(6, order.getBrand());
            preparedStatement.setString(7, order.getOrderTime());
            preparedStatement.setLong(8, order.getOrderTimeSeries());
            preparedStatement.setDouble(9, order.getPrice());
            preparedStatement.setInt(10, order.getNum());
            preparedStatement.setDouble(11, order.getTotalPrice());
            preparedStatement.setString(12, order.getStatus());
            preparedStatement.setString(13, order.getAddress());
            preparedStatement.setLong(14, order.getPhone());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
