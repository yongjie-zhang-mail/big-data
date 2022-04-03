package com.yj.bigdata.test

import org.apache.spark.sql.SparkSession

/**
 * @author zhangyj21
 */
object Test {
    
    def main(args: Array[String]): Unit = {
    
        val spark = SparkSession
          .builder
          //            .master("local")
          .master("yarn")
            .appName("HdfsTest")
            .getOrCreate()
    
        val filePath = args(0)
        //        val filePath = "./data/README.md"
        //        val rdd = spark.sparkContext.textFile(filePath)
        //        val lines = rdd.flatMap(x => x.split(" "))
        //            .map(x => (x, 1))
        //            .reduceByKey((a, b) => (a + b))
        //            .collect().toList
        //        println(lines)
    
        import spark.implicits._
        val ds = spark.read.textFile(filePath)
            .flatMap(x => x.split(" "))
            .map(x => (x, 1))
            .groupBy("_1")
            .count()
            .show()

    }
    
}
