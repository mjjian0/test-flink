package com.cj.flink.base

import java.util.Properties

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * Created by cj on 2019/11/12.
 */
class BaseService {

  /**
    * 覆盖增加conf配置
    *
    * @author Jiang Zhiyang @ 2019-10-30
    * @version 1.0.0
    */
  def reWriteConfig(args: Array[String]): Unit = {
    if (args != null && args.length > 0)
      ConfigManager.setPropertys(ParameterTool.fromArgs(args).toMap)
  }

  /**
    * 获取环境
    *
    * @author Jiang Zhiyang @ 2019-10-30
    * @version 1.0.0
    */
  def getStreamEnv(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //失败重启策略
    //env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, 9000))
//    val rocksDBStateBackend = new RocksDBStateBackend("hdfs:///home/task/flink/flink-checkpoints/checkpoint")
//    rocksDBStateBackend.enableTtlCompactionFilter();
//    env.setStateBackend(rocksDBStateBackend)

    env.enableCheckpointing(60 * 1000 * 1, CheckpointingMode.EXACTLY_ONCE)
    val config = env.getCheckpointConfig
    //RETAIN_ON_CANCELLATION在job canceled的时候会保留externalized checkpoint state
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，当指定这个参数时，maxConcurrentCheckpoints的值为1
    config.setMinPauseBetweenCheckpoints(3000)
    //用于指定运行中的checkpoint最多可以有多少个,如果有设置了minPauseBetweenCheckpoints，则maxConcurrentCheckpoints这个参数就不起作用了(大于1的值不起作用)
    config.setMaxConcurrentCheckpoints(1)
    //指定checkpoint执行的超时时间(单位milliseconds)，超时没完成就会被abort掉
    config.setCheckpointTimeout(60 * 1000 * 3)
    //用于指定在checkpoint发生异常的时候，是否应该fail该task，默认为true，如果设置为false，则task会拒绝checkpoint然后继续运行
    //https://issues.apache.org/jira/browse/FLINK-11662
    config.setFailOnCheckpointingErrors(false)
    env
  }

  def getKafkaConsumer(kafkaAddr: String, topicName: String, groupId: String): FlinkKafkaConsumer[ObjectNode] = {
    val properties = getKafkaProperties(groupId, kafkaAddr)
    val consumer = new FlinkKafkaConsumer[ObjectNode](topicName, new JsonNodeDeserializationSchema(), properties)
    consumer.setStartFromGroupOffsets() // the default behaviour
    consumer
  }

  def getKafkaProperties(groupId: String, kafkaAddr: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaAddr)
    properties.setProperty("group.id", groupId)
    properties.setProperty("max.request.size", String.valueOf(1048576 * 10))
    properties
  }

  /**
    * 默认7天过期
    *
    * @author Jiang Zhiyang @ 2019-11-7
    * @version 1.0.0
    */
  def getTTL(): StateTtlConfig = {
    getTTL(24 * 7)
  }

  def getTTL(expireHour: Int): StateTtlConfig = {
    //数据过期时间7天
    val ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.seconds(60 * 60 * expireHour))
      //Last access timestamp is initialised when state is created and updated on every write operation
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      //Never return expired user value.
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      //目前1.8只支持ProcessingTime，EventTime还在开发中
      .setTimeCharacteristic(StateTtlConfig.TimeCharacteristic.ProcessingTime)
      //https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/stream/state/state.html
      .cleanupInRocksdbCompactFilter()
      .build
    ttlConfig
  }
}
