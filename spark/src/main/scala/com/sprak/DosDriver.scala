package com.rl.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DosDriver {
  def main(args: Array[String]): Unit = {
     val parametres = new Parameters(args(0),args(1))
  
     val conf = new SparkConf().setMaster("local[2]").setAppName("DocCalcuationSpark")

     val sc = new SparkContext(conf)
     val bd = sc.broadcast(parametres)
    
     val foreCastFile = sc.textFile("file:///home/bhuvan/Desktop/sparkinput/forecastTest")
     val actuals = sc.textFile("file:///home/bhuvan/Desktop/sparkinput/actualTest")
    
     val DosWorker = new DosWorker(bd).processDos(actuals, foreCastFile)
     DosWorker.saveAsTextFile("file:///home/bhuvan/Desktop/sparkinput/ouput1")
  
  }
}