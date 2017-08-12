import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import scala.util.control.Breaks._
import java.util.Calendar
import java.text.SimpleDateFormat


object DosCalculationSpark {
  def main(args: Array[String]): Unit = {
   
    case class ForeCastData(year:Int,month:Int,week:Int,yearWeek:Int,quantity:Double)
    case class ActulasData(year:Int,month:Int,week:Int,yearWeek:Int,quantity:Double,dosType:String,date:String)
    case class Parameters(calcType:String,include:String) extends Serializable
   
    
     def getYearWeek(year:String,week:String):Int = {
      val newWeek  = if (week.length() == 1) "0" + week else week 
         (year + newWeek).toInt
         
    }
    
    
   
   val parametres = new Parameters(args(0),args(1))
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("DocCalcuationSpark")

    val sc = new SparkContext(conf)
    val bd = sc.broadcast(parametres)
    
    val foreCastFile = sc.textFile("file:///home/bhuvan/Desktop/sparkinput/forecast.csv")
    val actuals = sc.textFile("file:///home/bhuvan/Desktop/sparkinput/actuals.csv")
   
    
    val foreCastRDD = foreCastFile.map(_.split(",")).map(x => (x(0),ForeCastData(x(1).toInt,x(2).toInt,x(3).toInt,getYearWeek(x(1),x(3)),x(4).toDouble))).groupByKey().map(x => (x._1,x._2.toList))
    
    val actualRDD = actuals.map(_.split(",").toList).map(x => (x(0),ActulasData(x(1).toInt,x(2).toInt,x(3).toInt,getYearWeek(x(1),x(3)),x(4).toDouble,x(5),x(6))))
    
    val defaultForeCase = ForeCastData(0,0,0,0,0.0)
        
    
     
     def getDays(date:String,calType:String):Int ={
       
       val  c = Calendar.getInstance();
       val  dateFormat = new SimpleDateFormat("yyyy-MM-dd");
       val extractDate = dateFormat.parse(date);
       c.setTime(extractDate);
       
       val returnDate = calType.trim().toLowerCase()  match {
         case "week" => c.get(7)
         case "month" => c.getActualMaximum(5)
         case "date" => c.get(5)
         case _ => 0
       }
       returnDate
     
     }
   
def dosCal(key:String,act:ActulasData,forecastList:List[ForeCastData])={
   val sortedForeCase = forecastList.sortBy(f => f.yearWeek)
//   /sortedForeCase.foreach( println)
   
   val defaultDos = 728
   val noActual = 0
   var aquntity = act.quantity
   var previousQuantity = 0.0
   var previousFquantity = 0.0
   var numPeriod = 0
   
   
   
   
val dos = if ( act.quantity == 0 ) noActual else if (sortedForeCase(0).year == 0) defaultDos else  {
   
  
 for ( f <- sortedForeCase ){
   numPeriod = numPeriod +1
     
 if (aquntity > 0){
     
       previousFquantity =   if (f.quantity > 0) f.quantity else previousFquantity
       previousQuantity = aquntity
       aquntity = aquntity - f.quantity
   
 }else    
  numPeriod = numPeriod -  1
  

   }
 
 
  previousQuantity /previousFquantity
 
} 
   
numPeriod = numPeriod -1


val calcTypeNumber = if (bd.value.calcType.toLowerCase().trim() == "week") 7 else 30

var newCalcTypeFromExtractDate = if (numPeriod < 0 && calcTypeNumber == 7) getDays(act.date, bd.value.calcType) else calcTypeNumber 
newCalcTypeFromExtractDate = if (numPeriod < 0 && calcTypeNumber == 30) (getDays(act.date, "month")  - getDays(act.date, "date")) else calcTypeNumber


val remperiod = if (bd.value.include.toLowerCase().trim() == true) (forecastList(0).yearWeek - act.yearWeek) else 0 
val finalDos = dos + (numPeriod * calcTypeNumber) + remperiod    
finalDos  
}
    
    
    
    def checkForOption(x:Option[List[ForeCastData]],y:ForeCastData) = x match{
      case Some(x) => x
      case none => List(defaultForeCase)
    }
     
    
  //   val joinMap = actualRDD.leftOuterJoin(foreCastRDD).map(f => (f._1,(f._2._1,checkForOption(f._2._2, defaultForeCase))))
       val joinMap = actualRDD.leftOuterJoin(foreCastRDD).map(f => (f._1,(f._2._1,checkForOption(f._2._2, defaultForeCase))))
       
      // joinMap.foreach(println)
       
       val dos = joinMap.map(f => dosCal(f._1,f._2._1,f._2._2))
       
       println(s"= * ${50}  : ${dos.top(50)}"  ) 
       dos.foreach(println)
     

      
  }
  
  
  
}
