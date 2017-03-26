package com.sprak

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import java.sql.Connection
import java.sql.DriverManager
import org.apache.commons.lang.mutable.Mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object ClassificationDf2 {
  case class Employee(empid:String, name:String, location:String)

  def main(args: Array[String]): Unit = {
    println("BHUvan")
    // var bdMap = new mutable.HashMap[String, String]() 
    
    var brodcastMap = scala.collection.mutable.HashMap[String,String]()
    
    val url = "jdbc:mysql://localhost:3306/test"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val passwoed = "xxxxxxx"
    var connection: Connection = null
    try{
      
       Class.forName(driver)
      connection = DriverManager.getConnection(url, username, passwoed)
      val statement = connection.createStatement()
      val rs = statement.executeQuery("select * from config")
      while (rs.next()) {
         brodcastMap += (rs.getString(1) -> rs.getString(2))
      }
    }catch {
      case e: Throwable => e.printStackTrace() 
    }finally {
      connection.close()
    }
    
    
 println(brodcastMap.get("sse"))
    
 
     
    
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrames_Creation")

    val sc = new SparkContext(conf)
    val bd = sc.broadcast(brodcastMap)

    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val inputRdd = sc.textFile("/home/bhuvan/employee.csv").map(_.split(",")).filter ( _.length == 3).map (e => Employee(e(0),e(1),e(2))).toDF()
    inputRdd.registerTempTable("employee")
    
    println("Map is broadcasted" + bd.value.get("sse"))
    

     var dfdList =new ListBuffer[DataFrame]()
     
    
    bd.value.foreach{ f =>
      var key = f._1
      var queries = f._2      
     dfdList += sqlContext.sql(queries)
 }
  println("sixe is " + dfdList.size)
  // dfdList.map { x => x.show() }

  val uniondf = dfdList.reduce((a,b) => a unionAll(b))
  
  val spec1 = Window.partitionBy("empid").orderBy("_c2")
  
  //uniondf.withColumn( "rank", rank.over(spec1) ).show()
  val tempdf = uniondf.withColumn( "rank", rank.over(spec1))
  
  tempdf.filter(tempdf("rank") === 1).show()
  
  //uniondf.show()
  
  }
}
