package com.cj.flink.window

import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cj.flink.base.BaseService
import com.cj.flink.config.ConfigManager
import com.cj.flink.utils.{ComString, RedisUtil}
import javax.security.auth.login.Configuration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.{CoGroupFunction, FilterFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by cj on 2019/11/12.
 */
object SplicePayOrder extends BaseService {
  private val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    try {
      reWriteConfig(args)
      val groupId = ConfigManager.getProperty("groupid") //"producttest"
      val kafkaAddr = ConfigManager.getProperty("kafka.brokers")
      val headKey =ConfigManager.getProperty("headKey")// "scala_test"
      val detailGroup=ConfigManager.getProperty("detailGroup")
      val consumerGroup=ConfigManager.getProperty("consumerGroup")
      val env = getStreamEnv
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 3000))

      env.setStateBackend(new RocksDBStateBackend("hdfs://nameservice/flink/checkpoints", false))
      val config = env.getCheckpointConfig
      // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
      config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

      val orderConsumer = getKafkaConsumer(kafkaAddr, "t_order_consumer", consumerGroup)//"ConsumerToPayOrderTokafka-test1"
      val orderDetailConsumer = getKafkaConsumer(kafkaAddr, "t_order_detail", detailGroup)//"DetailToPayOrderTokafka-test1"
      val orderConsumerStream = env.addSource(orderConsumer).setParallelism(32).uid("splicepayorder_orderconsumer_stream").name("order_consumer_stream")
        .filter(value=>{
          val tp=value.get("tp").asText()
          val data=value.get("data")
          ("UPDATE".equals(tp)
            && value.get("updata").hasNonNull("pay_time")
            && value.get("updata").get("pay_time").asText == "") ||
            ("INSERT".equals(tp) && (!StringUtils.isEmpty(data.get("pay_time").asText())))
        })
        .assignAscendingTimestamps(value => {
          var time = 0l
          try time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value.get("data").get("modify_time").asText).getTime
          catch {
            case e: ParseException =>
              e.printStackTrace()
          }
          time
        })

      val orderDetailStream = env.addSource(orderDetailConsumer).setParallelism(32).uid("splicepayorder_orderdetail_stream").name("order_detail_stream")
        .filter(new FilterFunction[ObjectNode] {
          override def filter(value: ObjectNode): Boolean = {
            val oprType = value.get("tp").asText
            if (oprType == "UPDATE") {
              val data = value.get("data")
              if (value.get("updata").has("sub_order_status")
                && "4" == data.get("sub_order_status").asText
                && StringUtils.isEmpty(data.get("original_sub_order_id").asText)) {
                true
              }else   false
            }else   false
          }
        })
        .assignAscendingTimestamps(value => {
          var time = 0l
          try time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(value.get("data").get("modify_time").asText).getTime
          catch {
            case e: ParseException =>
              e.printStackTrace()
          }
          time
        })
      val produceConfig = getKafkaProperties(groupId, kafkaAddr)
      produceConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      produceConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      val producer =new FlinkKafkaProducer[String]("pay_order", new SimpleStringSchema, produceConfig)
      orderConsumerStream.coGroup(orderDetailStream)
        .where(value => {
          value.get("data").get("order_id").asText
        })
        .equalTo(value => {
          value.get("data").get("order_id").asText
        })
        .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
        .allowedLateness(Time.minutes(30))
        .apply(new InnerJoinFunction(headKey)).keyBy(_.get("sub_order_id"))
        .map(new organizationPayOrderFunction(headKey)).name("organizationPayOrder").setParallelism(10)
        .filter(value=>{StringUtils.isNotEmpty(value)}).setParallelism(10)
        .addSink(producer).setParallelism(32).name("payOrderToKafka")
      env.execute("SplicePayOrder")
    } catch {
      case e: Exception => log.error("任务运行异常:SplicePayOrder", e)
    }
  }

  class PayOrderPartition extends FlinkFixedPartitioner[Tuple2[String,String]](){
    override def partition(record: Tuple2[String,String], key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
      record._1.hashCode() % partitions.length
    }
  }

  class organizationPayOrderFunction(var headKey: String) extends RichMapFunction[util.HashMap[String, String], String] {
    private var redis: RedisUtil = null
    private var unRepeatKeyState:ValueState[String]=_
    private var unRepeatSuborderValueDes:ValueStateDescriptor[String]=_
    override def open(parameters: Configuration) {
      this.redis = new RedisUtil()
      this.unRepeatSuborderValueDes= new ValueStateDescriptor[String](ComString.SPLICEPAYORDER_UNREPEATSUBORDER_VALUESTATE,BasicTypeInfo.STRING_TYPE_INFO)
      unRepeatSuborderValueDes.enableTimeToLive(getTTL(3*24))
      this.unRepeatKeyState=getRuntimeContext.getState(unRepeatSuborderValueDes)
    }

    override def map(value: util.HashMap[String, String]): String = {
      var json = ""
      val subOrderId = value.get("sub_order_id").toString
      if(unRepeatKeyState==null||unRepeatKeyState.value()==null){
        val itemId = value.get("item_id").toString
        val barCode = value.get("barcode").toString
        var dealPrice = value.get("deal_price").toString.toDouble
        var strMeragep = redis.get("t_detail_expand_order" + subOrderId)
        strMeragep = if (!StringUtils.isEmpty(strMeragep)) strMeragep else "0"
        dealPrice = dealPrice - strMeragep.toDouble
        value.put("deal_price", dealPrice.toString)
        var buyer = "-1"
        val strBuyer = redis.get("t_product_detail_new"+ barCode)
        if (!StringUtils.isEmpty(strBuyer)) buyer = strBuyer
        value.put("buyer", buyer)
        var deptId = "-1"
        var deptName = "-1"
        var buyerGroupName = "-1"
        val strk = redis.get("bas_dapt_buyer_relation"+ buyer)
        if (!StringUtils.isEmpty(strk)) {
          val k = strk.split("_")
          deptId = k(0)
          deptName = k(1)
          buyerGroupName = k(2)
        }
        value.put("dept_id", deptId)
        value.put("dept_name", deptName)
        value.put("buyer_group_name", buyerGroupName)
        var cid1 = "-1"
        val strcid = redis.get("t_item_new" + itemId)
        if (!StringUtils.isEmpty(strcid)) cid1 = strcid
        value.put("cid1", cid1)
        var brandId = "-1"
        //品牌
        val strb = redis.get("t_item_bc" + itemId)
        if (!StringUtils.isEmpty(strb)) brandId = strb
        value.put("brand_id", brandId)
        json = JSON.toJSONString(value, SerializerFeature.QuoteFieldNames)
        println(json)
        unRepeatKeyState.update(subOrderId)
      }
      json
    }
  }

  class InnerJoinFunction(var headKey: String) extends CoGroupFunction[ObjectNode, ObjectNode, util.HashMap[String, String]] {
    this.headKey = headKey
    override def coGroup(first: java.lang.Iterable[ObjectNode], second: java.lang.Iterable[ObjectNode], out: Collector[util.HashMap[String, String]]): Unit = {
      import scala.collection.JavaConverters._
      val f = first.asScala.toList
      val s = second.asScala.toList
      if (f.nonEmpty && s.nonEmpty) for (t <- f) {
        for (x <- s) {
          val data2 = x.get("data")
          val subOrderId = data2.get("sub_order_id").asText
          val data1 = t.get("data")
          val orderId = data1.get("order_id").asText
          val d = new util.HashMap[String, String]
          d.put("order_id", orderId)
          d.put("consumer_id", data1.get("consumer_id").asText)
          d.put("pay_time", data1.get("pay_time").asText)
          d.put("buyer_province", data1.get("buyer_province").asText)
          d.put("buyer_city", data1.get("buyer_city").asText)
          d.put("order_type", data1.get("order_type").asText)
          d.put("es", t.get("es").asText)
          d.put("channel_id", data1.get("channel_id").asText)
          d.put("shop_id", data1.get("shop_id").asText)
          val totalPrice = data2.get("total_price").asDouble
          val dealPrice = totalPrice - data2.get("minus_value").asDouble
          d.put("sub_order_id", subOrderId)
          d.put("item_id", data2.get("item_id").asText)
          d.put("barcode", data2.get("barcode").asText)
          d.put("buy_count", data2.get("buy_count").asText())
          d.put("deal_price", dealPrice.toString)
          d.put("total_price", totalPrice.toString)
          d.put("bus_type", data2.get("bus_type").asText)
          d.put("store_code", data2.get("store_code").asText)
          d.put("shop_type", data2.get("shop_type").asText)
          d.put("json_create_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
          out.collect(d)
        }
      }
    }
  }

}
