package com.qiuhaijun.handler

import com.qiuhaijun.Bean.StartUpLog
import com.qiuhaijun.Utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
/**
  * 过滤逻辑的工具类
  * */
object DauHandler {
  /**
    * 同批次去重：注意同批次跨天的情况
    *
    * @param filterByRedis 根据Redis中的数据集，得到过滤后的结果
    */
  def filterDataByBatch(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {

    /**
      * 我们为什么将数据转换成((date,mid),log)，而不是(mid,log)？
      * 我们想到的是同一个批次内按mid分组，就能实现去重。但是当该批次是跨天的情况下，就有可能丢掉用户在下一天登录过一次的情况。
      * 举例：一个用户在1号23:59:59登录了一次，在2号00:00:3分的时候又登录了一次，这两次登录在同一个批次数据中。
      * 如果直接用mid分组去重，由于用的算子sortWith(_.ts < _.ts).take(1)，在迭代器中取最早的log，那么得到的就是1号登录的信息，而2号登录的信息就丢失了。
      * 所以我们的转换成((date,mid),log)这种形式，然后在1号和2号都去取一个最早的数据。
      * */
    //a.将数据转换为((date,mid),log)
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedis.map(log =>
      ((log.logDate, log.mid), log)
    )

    //b.按照key分组（相同日期相同mid的数据放到一个迭代器中）
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //c.组内按照日期排序，取第一个元素
    //    val value1: DStream[List[StartUpLog]] = dateMidToLogIterDStream.map { case ((_, _), logIter) =>
    //      val logs: List[StartUpLog] = logIter.toList.sortWith(_.logDate < _.logDate).take(1)
    //      logs
    //    }
    val value: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap { case ((_, _), logIter) =>
      val logs: List[StartUpLog] = logIter.toList.sortWith(_.ts < _.ts).take(1)
      logs
    }

    //d.返回数据
    value
  }


  /**
    * 跨批次去重(根据Redis中的数据进行去重)，当前批次数据与前面所有批次的数据结果进行比较。所以保存到
    * Redis中的数据，是每个批次的累计（即每个批次去重后，都要保存一次到Redis中）
    *
    * @param startLogDStream 从Kafka读取的原始数据
    */
  def filterDataByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    startLogDStream.transform(rdd => {

      //查询Redis数据，使用广播变量发送给Executor
      //获取连接
      //      val jedisClient: Jedis = RedisUtil.getJedisClient
      //      val ts: Long = System.currentTimeMillis()
      //date->今天/昨天
      //      val strings: util.Set[String] = jedisClient.smembers("dau:yesterdaydate")
      //      val strings1: util.Set[String] = jedisClient.smembers("dau:todaydate")
      //      Map[String, util.Set[String]]("yes" -> strings, "today" -> strings1)
      //释放连接
      //      jedisClient.close()

      rdd.mapPartitions(iter => {
        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //过滤
        /**
          * filter过滤，指的是得到过滤后的有效数据，而非得到过滤掉的无序数据。
          * sismember，判断传进来的kv是否在redis中存在，如果存在说明重复，返回为true；如果不存在说明不重复，返回false
          * 而我们需要不重复的数据，那么我们在这个方法的结果上取反。
          * */
        val logs: Iterator[StartUpLog] = iter.filter(log => {
          val redisKey = s"dau:${log.logDate}"
          !jedisClient.sismember(redisKey, log.mid)
        })
        //释放连接
        jedisClient.close()
        //返回过滤后的日志
        logs
      })
    })

  }


  /**
    * 将2次过滤后的数据集中的mid保存至Redis
    *
    * @param startLogDStream 经过2次过滤后的数据集
    */
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]) = {

    startLogDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(iter=>{

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //将数据写入Redis中
        iter.foreach(log=>{

          //设计redisKey(每天一个redisKey)
          val redisKey = s"dau:${log.logDate}"
          //将每条日志的redisKey和value存到redis中（用set集合来存value，天然去重。这里实现了两次去重，按天去重和mid去重）
          jedisClient.sadd(redisKey,log.mid)
        })

        //释放连接
        jedisClient.close()
      })
    })

  }

}
