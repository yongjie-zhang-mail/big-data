package com.yj.bigdata.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhangyj21
 */
object TestStreaming {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .master("local[*]")
          // .master("yarn")
          .appName("StreamingTest")
          .getOrCreate()
        // 构建 streaming context
        val sc = spark.sparkContext;
        val ssc = new StreamingContext(sc, Seconds(5))
        // 逻辑代码
        val lines = ssc.socketTextStream("mynode5", 9999)
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map(word => (word, 1))
        val wordCounts = pairs.reduceByKey(_ + _)
        wordCounts.print()
        // 启动
        ssc.start()
        ssc.awaitTermination()

    }

}
