package com.sprak
package com.sprak
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._

/**
  * Created by bhuvan on 26/3/17.
  */
object ClassificationDF {

  case class employee(empId:String, name:String, location:String)

  def main(args: Array[String]): Unit = {
    println("BHUvan")
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("DataFrames_Creation")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inputRdd = sc.textFile("/home/bhuvan/employee.csv")

    val inputRddStruct = inputRdd.map { record =>
      val fields = record.split(",")
      val empId = fields(0)
      val name = fields(1)
      val location = fields(2)

      employee(empId,name,location)
    }
      val employeeDF = inputRddStruct.toDF("empid","name","location")
      employeeDF.registerTempTable("emptable")


      val firstClassification = sqlContext.sql("select empid from emptable where empname id like 'rle%'")
    firstClassification.show()


  }

}
