package com.yj.bigdata.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.DriverManager

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
        val lines = ssc.socketTextStream("mynode2", 9999)
        val wordCounts = lines.flatMap(_.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
        // 控制台打印
        wordCounts.print()
        // 存入数据库
        wordCounts.foreachRDD(
            rdd => rdd.foreachPartition(
                rows => {
                    Class.forName("com.mysql.jdbc.Driver")
                    val conn = DriverManager.getConnection("jdbc:mysql://mynode2:3306", "root", "rooT-1234")

                    try {
                        for (row <- rows) {
                            val sql = "INSERT INTO kfk.word_count VALUES ('" + row._1 + "', '" + row._2 + "');"
                            conn.prepareStatement(sql).executeUpdate()
                        }
                    } finally {
                        if (conn != null) {
                            conn.close()
                        }
                    }
                }
            )
        )

        // 启动
        ssc.start()
        ssc.awaitTermination()

    }

}
