package com.qiuhaijun.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.qiuhaijun.Bean.StartUpLog
import com.qiuhaijun.Utils.MyKafkaUtil
import com.qiuhaijun.constants.GmallConstants
import com.qiuhaijun.handler.DauHandler
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
/**
  * 日活分时统计
  **/
object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestKafka")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //定义时间格式化对象(由于每一行的时间戳都要进行转换，所以定义一个全局的时间格式化对象）
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.取出value并打印
    //kafkaDStream.foreachRDD(_.foreach(println(_)))

    //将每一行数据转换成样例类对象(在这里面每添加一个功能，我们就用print（）打印一下。当功能添加完了，再删除print（），用var返回一个变量，用于下面的步骤)
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map {
      case x =>
        //获取json数据
        val str: String = x.value()
        //将json数据转换成样例类对象
        /**
          * 阿里的JSON工具类，可以实现样例类对象和json对象之间的转换
          * parseObject（）方法，实现将json数据转换成样例类对象。
          **/
        val log: StartUpLog = JSON.parseObject(str, classOf[StartUpLog])
        //取出时间戳
        val ts: Long = log.ts
        //将时间戳转换成字符串
        /**
          * 1.先将时间戳转换成日期对象：new Date(ts)
          * 2.定义时间格式化对象：new SimpleDateFormat("yyyy-MM-dd HH")
          * 3.再将格式化的日期转换成字符串（为了后面把天和小时都截取出来）
          *
          **/
        val logDateHour: String = dateFormat.format(new Date(ts))
        //切割开日期和小时
        val logDateHourArr: Array[String] = logDateHour.split(" ")
        //赋值日期和小时到样例类对象中
        log.logDate = logDateHourArr(0)
        log.logHour = logDateHourArr(1)

        log
    }

    //下面才是真正的业务，分时统计日活
    /**
      * 分析：
      * 每个时间段都要求一个日活，一个时间段有多个批次的数据，每个批次中有可能有重复数据。
      * 1.当前批次与上一批次数据去重（跨批次去重要用无状态，批次间无法进行通信，要借助第三方存储工具redis实现，跨批次去重。）
      * 2.一个批次中去重
      * 3.我们是将每个时间段的日活结果存到数据库中，还是将每个批次的不重复数据存到数据库中。
      *
      * 注意：我们用数据库来保存数据，我们还可以借助数据库的特点
      * 我们HBase中可以存海量数据，也就是存数据明细。我们可以把每个批次的有效数据都往HBase中存一次。
      * 等到我们需要计算某个时段的日活时，我们再利用phonix的SQL去查。
      *
      * 我们这里用的
      * 先批次间过滤，在批次内过滤，然后将数据存到HBase和redis中
      * 为什么先批次间过滤，再批次内过滤
      * 因为redis过滤的粒度大，一次过滤能过滤掉很多无效数据。
      *
      * 为什么过滤后的数据要保存到两个地方？
      * 因为每个批次保存到redis中的数据，会在下一个批次的过滤中用到。
      * 而HBase中的数据，是用来进行进一步分析用的数据。
      *
      **/
    //5.跨批次去重（借助Redis存储工具实现跨批次去重）
    /**
      * 注意：这里涉及到两种跨批次数据
      * 1.同一天里面的跨批次去重
      * 2.在零点左右，两个批次有可能跨天，这时候的去重
      *
      * 我们用每一个批次的每一行数据都去访问一次Redis来去重，就不会涉及到跨天的？
      **/
    val filterByRedis: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLogDStream)
    //测试用到的缓存
    filterByRedis.cache()

    //6.同批次去重
    val filterByBatch: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLogDStream)
    //测试用到的缓存
    filterByBatch.cache()//由于去重两次，所以把数据集进了缓存

    //7.将数据保存到redis中，以供下次去重使用
    /**
      * 单独用一个工具类来封装方法，来实现逻辑。
      *
      **/
    DauHandler.saveMidToRedis(filterByBatch)

    //测试(对比filterByBatch这边增加的数据条数，与Phoenix中数据增加的条数是否一致)
    //filterByRedis.print()
    //filterByBatch.print()
    //filterByRedis.count().print()
    filterByBatch.count().print()

    //8.有效数据写到HBase
    filterByBatch.foreachRDD(rdd =>
      rdd.saveToPhoenix("GMALL_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
    )

    //.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}


