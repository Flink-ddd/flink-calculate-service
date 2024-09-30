package com.panda.flink.sink;

import com.panda.flink.business.order.Order;
import com.panda.flink.business.user.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author muxiaohui
 */
public class JdbcUserWriter extends RichSinkFunction<User> {
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
        preparedStatement = connection.prepareStatement("insert into charts_user_info(user_id, user_name,gender,address,phone,age,register_time,register_label,register_source,register_time_series) values (?,?,?,?,?,?,?,?,?,?)");
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
     * @param user
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(User user, SinkFunction.Context context) throws Exception {
        try {
            //获取JdbcReader发送过来的结果
            preparedStatement.setString(1, user.getUserId());
            preparedStatement.setString(2, user.getUserName());
            preparedStatement.setString(3, user.getGender());
            preparedStatement.setString(4, user.getAddress());
            preparedStatement.setLong(5, user.getPhone());
            preparedStatement.setString(6, user.getAge());
            preparedStatement.setString(7, user.getRegisterTime());
            preparedStatement.setInt(8, user.getRegisterLabel());
            preparedStatement.setString(9, user.getRegisterSource());
            preparedStatement.setLong(10, user.getRegisterTimeSeries());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
