package com.bhuvan.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object NewBikeProblem {
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setMaster("local[2]").setAppName("New Bike Problem")   
    val sc = new SparkContext(conf)
    
    val sqlContext = new HiveContext(sc)
  
    import sqlContext.implicits._
    
    val travellData = sc.parallelize(List(("01-01-2017",50),
                                          ("01-02-2017",100),
                                          ("01-03-2017",200),
                                          ("01-03-2017",270))).toDF("date","kms")
                                          
val WinSpec1 = Window.orderBy("date")

val temp = travellData.withColumn("prevkms", lag(travellData("kms"), 1).over(WinSpec1)).
select($"date", $"kms",when(col("prevkms").isNull,$"kms").otherwise($"kms" - $"prevkms").alias("dailykms"))

temp.show()
    
    
    
  }
}