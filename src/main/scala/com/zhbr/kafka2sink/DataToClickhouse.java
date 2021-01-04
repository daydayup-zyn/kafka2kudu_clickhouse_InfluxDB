package com.zhbr.kafka2sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据保存到clickhouse (仅有insert语句)
 */
public class DataToClickhouse {

    public static void exeSql(String sql){
        //String address = "jdbc:clickhouse://192.168.72.142:8123/default";
        String address = ConfigsUtil.getClickhouseUrl();
        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(address);
            statement = connection.createStatement();
            long begin = System.currentTimeMillis();
            //results = statement.executeQuery(sql);
            boolean execute = statement.execute(sql);

            if (execute){
                System.out.println("数据插入成功");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {//关闭连接
            try {
                if(statement!=null){
                    statement.close();
                }
                if(connection!=null){
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
