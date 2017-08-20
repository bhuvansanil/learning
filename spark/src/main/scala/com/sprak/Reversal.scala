import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.lang.Double
import java.text.DecimalFormat
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map


object Reversal {
  def main(args: Array[String]): Unit = {
    
   
    case class key(sourceID:String,sourceRegion:String,source:String,region:String,env:String,werks:String,matnr:String,lifnr:String,ebeln:String,ebelp:String,menge:String) extends Serializable
    case class value(aedat:Int,aedatString:String,menge:Double,period:String,pyear:String,uom:String,periodTwoDigit:String,transactionReferNo:String,currency:String,ssePrice:String,mucofa:String,var reverseFlag:String,mengeString:String) extends Serializable
 
    
    
      val conf = new SparkConf().setMaster("local[2]").setAppName("Reversal")
      val sc = new SparkContext(conf)
    
    //  val inputFile = sc.textFile("/home/bhuvan/Desktop/input/tt.csv")
    
    val inputFile = sc.textFile("hdfs://nameservice1/user/barun/GR_prod_new")
      
      def formatMenge(x:String,y:Boolean) ={
        val doubleMenge = if ("\\N".equals(x) ) 0.0 else { if(y ==true)  Math.abs(x.toDouble) else x.toDouble}
        val decimalFormat = new DecimalFormat("0.00")
         decimalFormat.format(doubleMenge)       
    }
    
    def isNumeric(x:String) ={
      x != null && x.matches("[-+]?\\d*\\.?\\d+"); 
    }
    
    def formatAedat(s:String) ={
      
      if (isNumeric(s)) s.toInt else 0
      
    }
      
      //inputFile.foreach(println)
      
      
   
      val inputRDD = inputFile.map(_.split("\\|")).map(x=>((key(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(14),x(10),x(11),formatMenge(x(12),true)),
          value(formatAedat(x(7)),x(7),(formatMenge(x(12),false)).toDouble,x(9),x(8),x(13),x(15),x(16),x(17),x(18),x(19),"",x(12)))))      
        

     val groupRDD = inputRDD.groupByKey().map(x => (x._1,x._2.toList.sortBy(f => f.aedat)))  
          
    
     def printOutput(x:key,y:List[value]) ={
     
        val outputRDD = y.map(f => {
          x.sourceID + "|" + 
          x.sourceRegion + "|" + 
          x.source + "|" + 
          x.region + "|" + 
          x.env + "|" + 
          x.werks + "|" + 
          x.matnr + "|" + 
          f.aedatString + "|" + 
          f.pyear + "|" + 
          f.period + "|" + 
          x.ebeln + "|" + 
          x.ebelp + "|" + 
          f.mengeString + "|" + 
          f.uom + "|" + 
          x.lifnr + "|" + 
          f.periodTwoDigit + "|" + 
          f.transactionReferNo + "|" + 
          f.currency + "|" + 
          f.ssePrice + "|" + 
          f.mucofa + "|" + 
          f.reverseFlag 
        })
        
       outputRDD
      
        
      }
     
      def findReversalSize2(x:key,y:List[value])={
        if ((y(0).menge + y(1).menge ==0)) {
          y(0).reverseFlag = "X" 
          y(1).reverseFlag = "X"
        }
         val output = printOutput(x, y) 
         output
        
      }
      
      def findReversalSize3(x:key,y:List[value]) = {   
       
       val positiveList = y.filter(f => f.menge > 0).sortBy(f => f.aedat)
       val negativeList = y.filter(f => f.menge < 0).sortBy(f => f.aedat)

       negativeList.foreach(f  =>{
         
         val naedat = f.aedat
         var break = false
         for( d <- positiveList if (naedat >= d.aedat && !d.reverseFlag.equals("X") && break == false)){
          // if (naedat >= d.aedat && !d.reverseFlag.equals("X") && break == false){
             f.reverseFlag ="X"
             d.reverseFlag ="X"
             break = true         
           }
        // }
         
         
       }
       )
       
       val mergeList = positiveList ::: negativeList
       
       val output = printOutput(x, mergeList)
       
        output         
      }
      
      
     def findReversal(x:key,y:List[value]) ={
        
      val finalOuput =  if (y.size == 1) printOutput(x, y)
       else if (y.size ==2 ) findReversalSize2(x, y) 
       else if (y.size > 2) findReversalSize3(x, y)
       else
         List("null")
         
         finalOuput
      }
     
     
     //groupRDD.foreach(f => findReversal(f._1, f._2))
     
    val  saveToDiskRDD = groupRDD.flatMap(f => findReversal(f._1, f._2))
     
    //saveToDiskRDD.foreach(println)
    
    saveToDiskRDD.saveAsTextFile("hdfs://nameservice1/user/barun/testoutput")
    
    sc.stop()
     
     //  inputRDD.foreach(println)
      
  }
      
      
 
  
}