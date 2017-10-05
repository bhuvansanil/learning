package com.bhuvan.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SprkDfWindowFunction {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("Moving Average")
    
    val sc = new SparkContext(conf)
    
    val sqlContext = new HiveContext(sc)
  //  val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

     
    val customers = sc.parallelize(List(("Alice", "2016-05-01", 50.00),
                                    ("Alice", "2016-05-03", 45.00),
                                    ("Alice", "2016-05-04", 55.00),
                                    ("Bob", "2016-05-01", 25.00),
                                    ("Bob", "2016-05-04", 29.00),
                                    ("Bob", "2016-05-06", 27.00))).toDF("name", "date", "amountSpent")

                                    
   val WinSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
   
   // rows upper bound is -1 and lower bound is 1 so window is 3 ( upperbound,currntrow,lowerbound)
   // For Alice when in first row => upper bound -1, (no row before currnt row,50,45) => 50+45 /2 = 47.5
   // for Alice when in second row => ( 50,45,55) => 50+45+55 = 150/3 = 50
   // For Alice when in third row => (45,55,no row after currnt row) => 45+55 => 100 /2 = 50
   
   
   // calculate Moving average
  
   
   //code
   //customers.withColumn("movingavg", avg(customers("amountSpent")).over(WinSpec1)).show()
   
   
   
   //Output is
   /*
    * +-----+----------+-----------+---------+
| name|      date|amountSpent|movingavg|
+-----+----------+-----------+---------+
|Alice|2016-05-01|       50.0|     47.5|
|Alice|2016-05-03|       45.0|     50.0|
|Alice|2016-05-04|       55.0|     50.0|
|  Bob|2016-05-01|       25.0|     27.0|
|  Bob|2016-05-04|       29.0|     27.0|
|  Bob|2016-05-06|       27.0|     28.0|
+-----+----------+-----------+---------+
    */
                                    
                        
 //calculate cumulative sum
   
   val WinSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)
   
   
   //code
   //customers.withColumn("CumulativeSum",sum(customers("amountSpent")).over(WinSpec2)).show()

   
   
   //Cumulative sum = > sum of all the amountsent before + current row
   //window range = Long.Minval to 0 => 
   //For Alice when in first row (No rows above current  row +50(currnt row)) => (0 + 50) = 50
   //for Alice when in second Row(1 row above current row + 45(currnt row)) => (50 +45 ) = 95
   //for alice when in third row( 2 row above currnet row + 55(currrnmt row)) => (50 +45 + 55) = 150
   
   //output
   /*+-----+----------+-----------+-------------+
| name|      date|amountSpent|CumulativeSum|
+-----+----------+-----------+-------------+
|Alice|2016-05-01|       50.0|         50.0|
|Alice|2016-05-03|       45.0|         95.0|
|Alice|2016-05-04|       55.0|        150.0|
|  Bob|2016-05-01|       25.0|         25.0|
|  Bob|2016-05-04|       29.0|         54.0|
|  Bob|2016-05-06|       27.0|         81.0|
+-----+----------+-----------+-------------+
    * 
    */
   
   //Data from previous row
   
   
   val Winspec3 = Window.partitionBy("name").orderBy("date")
   
   val temp = customers.withColumn("previousColumn", lag(customers("amountSpent"),1).over(Winspec3)).
   select($"name",$"date",$"amountSpent",when(col("previousColumn").isNull,0).otherwise(col("previousColumn")))
   temp.show()
   //when(col(previousColumn).isNull 0)
   
   

    
    
  }
  
  
}