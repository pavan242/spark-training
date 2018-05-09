package com.pavan.spark_training.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    .setAppName("Word Count")
    .setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    
    if (args.length < 1) {
      println("Missing Arguments !!!!!")
    }
    
    val textFile = sc.textFile(args(0))
    
    val word_count = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    
    val sorted = word_count.sortByKey()
    
    //sorted.foreach(println)
    sorted.collect.foreach(println)
    
    //sorted.collect.saveAsTextFile(args(1))
    sc.stop()
  }
}