package com.bhuvan.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    
    
    
    val list = sc.parallelize(List("yarn child","yarn child one","yarn mother"))
    
    val splitList = list.flatMap(_.split(" "))//.map(f=> (f,1)).reduceByKey(_+_)
    
    splitList.foreach(println)
    
    
    
    
    
  }
  
  
}