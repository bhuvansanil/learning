package com.sprak
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._

/**
  * Created by bhuvan on 26/3/17.
  */
object ClassificationDF {

  case class Employee(empid:String, name:String, location:String)

  def main(args: Array[String]): Unit = {
    println("BHUvan")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrames_Creation")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputRdd = sc.textFile("/home/bhuvan/employee.csv").map(_.split(",")).filter ( _.length == 3).map (e => Employee(e(0),e(1),e(2))).toDF()
    inputRdd.registerTempTable("emptable")
    
    //val structRdd = inputRdd.toDF()
    
   // inputRdd.foreach { println }
    
    
    
    //val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    
    

 /*   val inputRddStruct = inputRdd.map { record =>
      val fields = record.split(",")
      val empId = fields(0)
      val name = fields(1)
      val location = fields(2)

      employee(empId,name,location)
    }
    
      val employeeDF = inputRddStruct.toDF("empid","name","location")
      employeeDF.registerTempTable("emptable")*/


      val firstClassification = sqlContext.sql("select empid,1 from emptable where name  like 'bhu%'")
    firstClassification.show()


  }

}
