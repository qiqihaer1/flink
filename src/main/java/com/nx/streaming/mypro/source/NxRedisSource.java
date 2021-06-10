package com.nx.streaming.mypro.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

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
public class NxRedisSource implements SourceFunction<HashMap<String,String>> {

    private Logger logger=LoggerFactory.getLogger(NxRedisSource.class);

    private Jedis jedis;
    private boolean isRunning=true;

    @Override
    public void run(SourceContext<HashMap<String, String>> cxt) throws Exception {
        this.jedis = new Jedis("10.0.0.9",6379);
        HashMap<String, String> map = new HashMap<>();
        while(isRunning){
          try{
              map.clear();
              Map<String, String> areas = jedis.hgetAll("areas");
              for(Map.Entry<String,String> entry: areas.entrySet()){
                  String area = entry.getKey();
                  String value = entry.getValue();
                  String[] fields = value.split(",");
                  for(String country:fields){
                      //
                      map.put(country,area);
                  }

              }
              if(map.size() > 0 ){
                  cxt.collect(map);
              }
              Thread.sleep(60000);
          }catch (JedisConnectionException e){
              logger.error("redis连接异常",e.getCause());
              this.jedis = new Jedis("192.168.167.254",6379);
          }catch (Exception e){
              logger.error("数据源异常",e.getCause());
          }

        }

    }

    @Override
    public void cancel() {
        isRunning=false;
        if(jedis != null){
            jedis.close();
        }

    }
}
