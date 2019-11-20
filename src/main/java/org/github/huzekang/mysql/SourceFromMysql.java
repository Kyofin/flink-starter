package org.github.huzekang.mysql;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @program: flink-starter
 * @author: huzekang
 * @create: 2019-11-20 15:47
 **/
public class SourceFromMysql extends RichSourceFunction<Map> {

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(connection)) {
            connection.close();
        }
        if (Objects.nonNull(ps)) {
            ps.close();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = this.getConnection();
        String sql = "select * from t_csv1";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        ResultSet rs = ps.executeQuery();
        // 获得结果集结构信息（元数据）
        ResultSetMetaData md = rs.getMetaData();
        // ResultSet列数
        int columnCount = md.getColumnCount();
        // ResultSet转List<Map>数据结构
        // next用于移动到ResultSet的下一行，使下一行成为当前行
        while (rs.next()) {
            Map<Object, Object> map = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                // 遍历获取对当前行的每一列的键值对，put到map中
                // rs.getObject(i) 获得当前行某一列字段的值
                map.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
            }
            ctx.collect(map);
        }
    }

    @Override
    public void cancel() {

    }

    private Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.1.150:3306/test?serverTimezone=UTC&characterEncoding=utf-8","root","root");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;

    }
}
