package com.cj.flink.stream;

import com.alibaba.fastjson.JSONObject;
import com.cj.utils.CommonUtil;
import com.cj.utils.DBManager;
import com.cj.utils.RedisClusterApi;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class BSHourCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 3000));
        env.enableCheckpointing(1000);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonUtil.brokerServers3);//""
        properties.setProperty("zookeeper.connect", CommonUtil.zks);//""
        properties.setProperty("group.id", "flink-stream-hourcount-test");//flink consumer flink的消费者的group.id

        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09("pay_order3", new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();
        /*String p1=args[0];
        if(p1.equals("0")){
            myConsumer.setStartFromEarliest();      // 设置从最早。使用这个方法的话，Kafka中提交的位移就将会被忽略而不会被用作起始位移
        }else if(p1.equals("1")){
            myConsumer.setStartFromLatest();        // 最新位移处开始消费。使用这两个方法的话，Kafka中提交的位移就将会被忽略而不会被用作起始位移
        }else{
            myConsumer.setStartFromGroupOffsets();  // 这是默认情况，即从消费者组提交到Kafka broker上的位移开始读取分区数据（对于老版本而言，位移是提交到Zookeeper上）。如果未找到位移，使用auto.offset.reset属性值来决定位移。该属性默认是LATEST，即从最新的消息位移处开始消费
        }*/
//        myConsumer.assignTimestampsAndWatermarks(new CustomWaterMark());
        //env.addSource(myConsumer).setParallelism(2);
        DataStream<String> stream =env.addSource(myConsumer).setParallelism(10);
//                env.fromElements("{\"deptName\":\"超市食品\",\"bus_type\":\"0\",\"total_price\":29.9,\"buyer_province\":\"江苏省\", \"item_id\":\"116351\",\"buy_count\":\"1\",\"deptId\":\"23\",\"sub_order_id\":\"YJCS54396726755522105344_0\",  \"buyer_city\":\"盐城市\",\"buyerGroupName\":\"休闲食品\",\"pay_time\":\"2019-04-13 10:33:57\",\"buyer\":\"124\",  \"shop_id\":\"5439672\",\"json_createTime\":\"2019-04-13 10:34:08.975\",\"order_id\":\"YJCS54396726755522105344\",  \"cid1\":\"91\",\"order_type\":\"YJCS\",\"barcode\":\"35000396025111\"}"
//                ,"{\"deptName\":\"超市食品\",\"bus_type\":\"0\",\"total_price\":30.9,\"buyer_province\":\"江苏省\", \"item_id\":\"116351\",\"buy_count\":\"1\",\"deptId\":\"23\",\"sub_order_id\":\"YJCS54396726755522105344_0\",  \"buyer_city\":\"盐城市\",\"buyerGroupName\":\"休闲食品\",\"pay_time\":\"2019-04-13 10:33:57\",\"buyer\":\"124\",  \"shop_id\":\"5439672\",\"json_createTime\":\"2019-04-13 10:34:08.975\",\"order_id\":\"YJCS54396726755522105344\",  \"cid1\":\"91\",\"order_type\":\"YJCS\",\"barcode\":\"35000396025111\"}"
//        );
//        stream.print().setParallelism(2);

        stream.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                boolean s=(!value.contains("\"cid1\":\"293\""))&&(value.contains("\"bus_type\":\"0\"")||value.contains("\"bus_type\":\"1\""));
                return s;
            }
        }).map(new RichMapFunction<String, Tuple5<String,Double,Integer,String,Integer>>() {

            @Override
            public Tuple5<String,Double,Integer,String,Integer> map(String value) throws Exception {
                JSONObject jsonObject= JSONObject.parseObject(value);
                String payHour=jsonObject.getString("pay_time").substring(0,13).replace("-","").replace(" ","");
                double totalPrice=Double.valueOf(jsonObject.getString("total_price"));
                int buyCount=Integer.valueOf(jsonObject.getString("buy_count"));
                String orderId=jsonObject.getString("order_id");
                return new Tuple5<String, Double, Integer, String,Integer>(payHour,totalPrice,buyCount,orderId,0);
            }
        }).keyBy(0).reduce(new RichReduceFunction<Tuple5<String, Double, Integer, String,Integer>>() {
            private RedisClusterApi redis=new RedisClusterApi();

            public Tuple5<String, Double, Integer,String,Integer> reduce(Tuple5<String, Double, Integer, String,Integer> value1, Tuple5<String, Double, Integer, String,Integer> value2) throws Exception {
               String payHour= value1.f0;
               double totalprice1=value1.f1;
               int buyCount1=value1.f2;
               String orderId1=value1.f3;
                double totalprice2=value2.f1;
                int buyCount2=value2.f2;
                String orderId2=value2.f3;
                double totalprice=totalprice1+totalprice2;
                int buyCount=buyCount1+buyCount2;
                if(!StringUtils.isEmpty(orderId1)){
                    redis.sadd("flink-state-test"+payHour,orderId1);
                }
                redis.sadd("flink-state-test"+payHour,orderId2);
                redis.expire("flink-state-test"+payHour,60);
                int orderNum=Integer.valueOf(String.valueOf(redis.scard("flink-state-test"+payHour)));
                return new Tuple5<String, Double, Integer, String,Integer>(payHour,totalprice,buyCount,orderId2,orderNum);
            }
        }).addSink(new RichSinkFunction<Tuple5<String, Double, Integer,String,Integer>>() {
              private static final long serialVersionUID = 1L;
              private Connection connection;
              private PreparedStatement preparedStatement;
              private long i=0;
              @Override
              public void open(Configuration parameters) throws Exception {
                  connection = DBManager.getConn();
              }

              @Override
              public void close() throws Exception {
                  if (preparedStatement != null) {
                      preparedStatement.close();
                  }

                  if (connection != null) {
                      connection.close();
                  }
              }


              public void invoke(Tuple5<String, Double, Integer,String,Integer> value) throws Exception {
                  try{
                      if (connection == null) {
                          connection = DBManager.getConn();//DriverManager.getConnection(sConf.URL, sConf.USERNAME, sConf.PASSWORD);
                      }
                      i++;
                      //payHour,totalprice,buyCount,orderId2,orderNum
                      String sql = "insert into dw_order_hour_sale (pay_hour,total_order_num,total_sale,item_buy_num,create_time) values (?,?,?,?,?) ON DUPLICATE KEY UPDATE total_order_num=?,total_sale=?,item_buy_num=?,create_time=?";
                      preparedStatement = connection.prepareStatement(sql);
                      preparedStatement.setString(1, value.f0);
                      preparedStatement.setInt(2,value.f4);
                      preparedStatement.setDouble(3,value.f1);
                      preparedStatement.setInt(4, value.f2);
                      preparedStatement.setString(5, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                      preparedStatement.setInt(6, value.f4);
                      preparedStatement.setDouble(7,value.f1);
                      preparedStatement.setInt(8,value.f2);
                      preparedStatement.setString(9, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                      preparedStatement.executeUpdate();
                  }catch (Exception e){
                      e.printStackTrace();
                  }
              }
          });
        env.execute("flink-hourcount-test");
    }
}
