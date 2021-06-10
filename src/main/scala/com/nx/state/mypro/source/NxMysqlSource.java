package com.nx.state.mypro.source;

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

    private Connection getConnection() {
        connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://vmmaster:3306/bigdata?useUnicode=true&characterEncoding=UTF-8";
            String username = "root";
            String password = "Symbio@123";
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }

    // 用来建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "select data_scope_key,site_display_name from sys_vendor_lob_site";
        ps = this.connection.prepareStatement(sql);
        System.out.println("open");
    }


    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        HashMap<String, String> map = new HashMap<>();
        while (resultSet.next()) {
            String dataScopeKey = resultSet.getString("data_scope_key");
            String siteName = resultSet.getString("site_display_name");
            map.put(dataScopeKey, siteName);
        }
        sourceContext.collect(map);
    }


//    @Override
//    public void run(SourceContext<HashMap<String, String>> cxt) throws Exception {
//        this.jedis = new Jedis("10.0.0.9",6379);
//        HashMap<String, String> map = new HashMap<>();
//        while(isRunning){
//          try{
//              map.clear();
//              Map<String, String> areas = jedis.hgetAll("areas");
//              for(Map.Entry<String,String> entry: areas.entrySet()){
//                  String area = entry.getKey();
//                  String value = entry.getValue();
//                  String[] fields = value.split(",");
//                  for(String country:fields){
//                      //
//                      map.put(country,area);
//                  }
//
//              }
//              if(map.size() > 0 ){
//                  cxt.collect(map);
//              }
//              Thread.sleep(60000);
//          }catch (JedisConnectionException e){
//              logger.error("redis连接异常",e.getCause());
//              this.jedis = new Jedis("192.168.167.254",6379);
//          }catch (Exception e){
//              logger.error("数据源异常",e.getCause());
//          }
//
//        }
//
//    }


    @Override
    public void cancel() {
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
