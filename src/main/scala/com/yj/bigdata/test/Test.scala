package com.yj.bigdata.test

import org.apache.spark.sql.SparkSession

/**
 * @author zhangyj21
 */
object Test {
    
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
            .builder
            .appName("HdfsTest")
            .getOrCreate()
        
        
    }
    
}
