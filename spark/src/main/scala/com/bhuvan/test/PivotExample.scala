package com.bhuvan.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object PivotExample {
  
 def main(args: Array[String]): Unit = {
   
   //Input csv file
   /*
user|movie|rating
bhuvan|Dark Hour|7
bhuvan|ted1|8
bhuvan|ted2|7
bhuvan|iron man|9
Santosa|Dark Hour|6
Santosa|Masti|8
Santosa|KAL|5
Santosa|karadi|8
GB|Finest Hour|9
GB|mabbu|10
GB|mabbu2|10
GB|black dog|6
haneep|Dark Hour|10
haneep|Drak value|7
haneep|breaking bad|10
haneep|GOT|10
haneep|GOT|10
    * 
    * 
    */
   
   val conf = new SparkConf().setAppName("Pivot Example").setMaster("local[2]")
   val sc = new SparkContext(conf)
   val sqlContext = new HiveContext(sc)
   
   import sqlContext.implicits._
   
   val movieDf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "|").
   option("header","true").option("inferschema", "true").load("/home/bhuvan/Desktop/movie.csv")
   
  // movieDf.show()
   
   val moviepivot = movieDf.groupBy("user").pivot("movie").agg(expr("coalesce(first(rating),0)").cast("double"))
   
  moviepivot.show()
   sc.stop()
   
   
   //Ouput 
   /*
    * 
+-------+---------+----------+-----------+----+---+-----+---------+------------+--------+------+-----+------+----+----+
|   user|Dark Hour|Drak value|Finest Hour| GOT|KAL|Masti|black dog|breaking bad|iron man|karadi|mabbu|mabbu2|ted1|ted2|
+-------+---------+----------+-----------+----+---+-----+---------+------------+--------+------+-----+------+----+----+
| haneep|     10.0|       7.0|        0.0|10.0|0.0|  0.0|      0.0|        10.0|     0.0|   0.0|  0.0|   0.0| 0.0| 0.0|
|Santosa|      6.0|       0.0|        0.0| 0.0|5.0|  8.0|      0.0|         0.0|     0.0|   8.0|  0.0|   0.0| 0.0| 0.0|
|     GB|      0.0|       0.0|        9.0| 0.0|0.0|  0.0|      6.0|         0.0|     0.0|   0.0| 10.0|  10.0| 0.0| 0.0|
| bhuvan|      7.0|       0.0|        0.0| 0.0|0.0|  0.0|      0.0|         0.0|     9.0|   0.0|  0.0|   0.0| 8.0| 7.0|
+-------+---------+----------+-----------+----+---+-----+---------+------------+--------+------+-----+------+----+----+
    */
   
 } 
 
}