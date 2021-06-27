package com.nx.state.mypro.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;

/**
 *
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 *
 * HashMap
 *
 * US，AREA_US
 * TW，AREA_CT
 * HK，AREA_CT
 *
 *
 *
 *
 */
public class NxMysqlSource extends RichParallelSourceFunction<HashMap<String, String>> {

    private Logger logger=LoggerFactory.getLogger(NxMysqlSource.class);

    private PreparedStatement ps;
    private Connection connection;
    private boolean running = true;//用于判断是否正常发送数据

    private Connection getConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://vmmaster:3306/bigdata?useUnicode=true&characterEncoding=UTF-8";
            String username = "root";
            String password = "Symbio@123";
            this.connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }

    // 用来建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        this.connection = getConnection();
        System.out.println("open");
    }


    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        String sql = "select data_scope_key,site_display_name from sys_vendor_lob_site";
        while (running){//定义无限循环，不停产生数据，除非被cancell
            ps = this.connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            HashMap<String, String> map = new HashMap<>();
            while (resultSet.next()) {
                String dataScopeKey = resultSet.getString("data_scope_key");
                String siteName = resultSet.getString("site_display_name");
                map.put(dataScopeKey, siteName);
            }
            sourceContext.collect(map);
        }


    }


    @Override
    public void cancel() {
        running = false;
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}


