package com.pavan.spark_training.sparkexamples

import org.apache.spark.sql.SparkSession

object learningjournal01 {
  def main(args: Array[String]): Unit = {
    
    val sparkSession = SparkSession.builder
                       .master("local[*]")
                       .appName("learning Journal example 01")
                       .getOrCreate()

    /*val df = sparkSession.read.options( Map("header" -> "true",
                                     "inferSchema" -> "true", 
                                     "nullValue" -> "NA",
                                     "timestampFormat" -> "yyyy-MM-dd'T'HH:mm?:ss",
                                     "mode" -> "failfast")
                                ).csv(args(0))*/
     
     val df = sparkSession.read
                  .format("csv")
                  .option("header","true")
                  .option("inferSchema","true") 
                  .option("nullValue","NA")
                  .option("timestampFormat","yyyy-MM-dd'T'HH:mm?:ss")
                  .option("mode","failfast")
                  .option("path",args(0))
                  .load()

    df.select("Timestamp", "Age","remote_work","leave").filter("Age > 30").show

    sparkSession.stop
  
  }
}