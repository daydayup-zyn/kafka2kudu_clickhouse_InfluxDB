package com.zhbr.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.beans.PropertyVetoException;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 *  1. 获取数据源
 *  2. 获取连接对象
 *  3. 返回dbutils的QueryRunner对象
 * @ClassName JDBCUtil
 * @Description TODO
 * @Autor yanni
 * @Date 2020/5/5 12:17
 * @Version 1.0
 */
public class JDBCUtil {
    private static ComboPooledDataSource dataSource = null;

    static {
        //读取配置文件
        String p_dir = System.getProperty("user.dir");
        Properties properties = new Properties();
        try {
            // 使用InPutStream流读取properties文
            BufferedReader bufferedReader = new BufferedReader(new FileReader(p_dir+"/app.properties"));
            // 使用 properties 对象加载输入流
            properties.load(bufferedReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(properties.getProperty("db.url"));
        dataSource.setUser(properties.getProperty("db.userName"));
        dataSource.setPassword(properties.getProperty("db.password"));
        dataSource.setInitialPoolSize(2);
        dataSource.setMaxIdleTime(30);
        dataSource.setMaxPoolSize(10);
        dataSource.setMinPoolSize(2);
        dataSource.setMaxStatements(50);
        try {
            dataSource.setDriverClass(properties.getProperty("db.driver"));
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
    }
    /**
     * 1.获取数据库连接对象 （事务），这个连接对象需要手动释放
     * @return
     */
    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 2.获取dbutils的QueryRunner对象（非事务），资源会自动释放
     * @return
     */
    public static QueryRunner getQueryRunner() {
        return new QueryRunner(dataSource);
    }
}
