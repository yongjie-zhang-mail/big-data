package com.yj.bigdata.test

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @author zhangyj21
 */
object Test2 {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          //            .master("local")
          .master("yarn")
          .appName("HdfsTest")
          .getOrCreate()

        // 从 hive 表读取数据表，存入 mysql 表
        val result = spark.sql("select * from kfk.test")
        var props = new Properties()
        //props.setProperty("username","root")
        //props.setProperty("password","rooT-1234")
        props.setProperty("driver", "com.mysql.jdbc.Driver")
        result.write.jdbc("jdbc:mysql://mynode2:3306/kfk?user=root&password=rooT-1234", "kfk.dept1", props)


    }

}
