package com.rl.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import java.util.Calendar
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import java.util.Formatter.DateTime
import org.joda.time.Weeks
import org.joda.time.Months

class DosWorker(bd:Broadcast[Parameters]) extends Serializable{
   case class ForeCastData(year:Int,month:Int,week:Int,period:Int,quantity:Double)
   case class ActulasData(key:String,year:Int,month:Int,week:Int,period:Int,quantity:Double,dosType:String,date:String)
   val defaultForeCase = ForeCastData(0,0,0,0,0.0)
   
   
   def getPeriod(year:String,week:String):Int = {
      val newWeek  = if (week.length() == 1) "0" + week else week 
         (year + newWeek).toInt
         
    }
   
   def findPeriod(year:String,month:String,week:String):Int = {
     
     if (week.toInt == -1) getPeriod(year, month) else getPeriod(year, week)
     
   }
   
   def getCalcTyp(week:Int) = {
     if (week == -1) 30 else 7
     
   }
   
   def checkForOption(x:Option[List[ForeCastData]],y:ForeCastData) = x match{
      case Some(x) => x
      case none => List(defaultForeCase)
    }
   
    def getDays(date:String,calType:String):Int ={    
       val  c = Calendar.getInstance();
       val  dateFormat = new SimpleDateFormat("yyyy-MM-dd");
       val extractDate = dateFormat.parse(date);
       c.setTime(extractDate);
       
       val returnDate = calType.trim().toLowerCase()  match {
         case "week" => {
           c.get(7)  match {
             case 1 => 7
             case _ => c.get(7) -1
           }
         }
         case "month" => c.getActualMaximum(5)
         case "date" => c.get(5)
         case _ => 0
       }
       
       
       
       println(returnDate)
       returnDate
     
     }
    
  def getperiodDifference(ayear:Int,aweek:Int,amonth:Int,fyear:Int,fweek:Int,fmonth:Int,calcType:String)= {
    val actualCalendar = Calendar.getInstance()
    val foreCastCalendar = Calendar.getInstance()
    
    actualCalendar.set(Calendar.YEAR, ayear)
    foreCastCalendar.set(Calendar.YEAR, fyear)
    
   if (calcType == "week") {
      actualCalendar.set(Calendar.WEEK_OF_YEAR, aweek)
      foreCastCalendar.set(Calendar.WEEK_OF_YEAR, fweek)
    }
    else
    {
      actualCalendar.set(Calendar.MONTH,amonth)
      foreCastCalendar.set(Calendar.MONTH, fmonth)     
    }
  
  val foreCastDateTime = new DateTime(foreCastCalendar);
	val actualDateTime  = new DateTime(actualCalendar);
	
	
	val ouput = if (calcType == "week")  {
	  Weeks.weeksBetween(actualDateTime, foreCastDateTime).getWeeks();
	}else{
	  Months.monthsBetween(actualDateTime, foreCastDateTime).getMonths();
	}
	ouput
  }
  
  
/*  def findMissingData(syear:Int,sWeek:Int,sMonth:Int,eYear:Int,yWeek:Int,yMonth:Int,calcType:String)={
    
    
    
    
  }*/
  
 def dosCal(key:String,act:ActulasData,forecastList:List[ForeCastData])={
   val sortedForeCase = forecastList.sortBy(f => f.period)
   val calcTypeNumber = getCalcTyp(act.week)
   val calcTypeString = if (calcTypeNumber == 7) "week" else "month"
  // val lengthofForeCast = sortedForeCase.size - 1
   
   /* for (i <- 0 to lengthofForeCast){
      
      if ((i+1) <= lengthofForeCast){
        findMissingData(sortedForeCase(i).year, sortedForeCase(i).week, sortedForeCase(i).month, sortedForeCase(i+1).year, sortedForeCase(i+1).week, sortedForeCase(i+1).month, calcTypeString)
        
        
      }
      
      
      
    }
     */
   val defaultDos = 728
   val noActual = 0
   var aquntity = act.quantity
   var previousQuantity = 0.0
   var previousFQtyTwo = 0.0
   var previousFquantity = 0.0
   //var previousFquantity = 0.0
   var numPeriod = 0
   var remPeriodObj= sortedForeCase(0)
   var remainingPeriodFlag = true
   
 
   
val dos = if ( act.quantity == 0 ) noActual else if (sortedForeCase(0).year == 0) defaultDos else  {  
 var break = false
 
 for ( f <- sortedForeCase){   
 if (aquntity > 0 ){
      if (break == false && (f.period > act.period)){
       if (remainingPeriodFlag) {
         remPeriodObj = f
         remainingPeriodFlag = false
       }
       
        
       numPeriod = numPeriod + 1
       previousFQtyTwo = previousFquantity
       previousFquantity =   (if (f.quantity > 0) f.quantity else previousFquantity)
       previousQuantity = aquntity
       aquntity = aquntity - f.quantity
      }
 }else    break = true
   }
 
 
  if (numPeriod > 0) numPeriod = numPeriod - 1
  
 val tempDOS = if (previousFquantity == 0) defaultDos
 else {
   if (numPeriod == 0) (previousQuantity/previousFquantity) * 7
   else
   (previousQuantity/previousFQtyTwo) * 7
 }
   
 tempDOS 
  
/* val tempDOS = if (numPeriod > 0){
   if (previousFquantity(previousFquantity.size -1) ==0) defaultDos 
 }
 else if (numPeriod == 0) previousQuantity /previousFquantity(previousFquantity.size -1)
 else
   previousQuantity /previousFquantity(previousFquantity.size -2)*/
 
 
/* numPeriod  match {
   case 0 => previousQuantity /previousFquantity(previousFquantity.size -1)
   case _ => if (numPeriod > 0) && 
 }*/



} 
   
  
 val defaultDosFlag = if ( ( act.quantity == 0 ) || (sortedForeCase(0).year == 0) || (numPeriod > 0 && previousFQtyTwo ==0) ) true else false
 
 val modifyFlag = if (numPeriod == 0) false else true

val remperiod = getperiodDifference(act.year, act.week, act.month, remPeriodObj.year, remPeriodObj.week, remPeriodObj.month, calcTypeString)  


/*val dosAfter = if (defaultDosFlag) dos else if  (!modifyFlag) dos  else {
   dos + ( if (calcTypeString == "week") ((7 - getDays(act.date, "week")) + 1 ) else (getDays(act.date, "month")  - getDays(act.date, "date")))
 }*/
 
 
 

val finalDos = if (defaultDosFlag) dos  else { 
  if (remperiod > 0) {
    (numPeriod * calcTypeNumber) + ((remperiod -1)  *  calcTypeNumber) + dos
  }
  else
    (numPeriod * calcTypeNumber) + dos
}

val finalDosOutput = if (defaultDosFlag) dos else if  (!modifyFlag) dos  else {
  finalDos -( if (calcTypeString == "week") {
    (getDays(act.date, "week")) 
  } 
   else (getDays(act.date, "month")  - getDays(act.date, "date")))
}


act.key + "|" +  act.year + "|" + act.month + "|" + act.period + "|" + act.quantity + "|"  + act.dosType + "|" + act.date + "|" + math.floor(finalDosOutput)
}
  
 
 
 def processDos(actualFile:RDD[String],foreCastFile:RDD[String])={
    
      val foreCastRDD = foreCastFile.map(_.split(",")).map(x => (x(0),ForeCastData(x(1).toInt,x(2).toInt,x(3).toInt,findPeriod(x(1),x(2),x(3)),x(4).toDouble))).groupByKey().map(x => (x._1,x._2.toList))
      val actualRDD = actualFile.map(_.split(",").toList).map(x => (x(0),ActulasData(x(0),x(1).toInt,x(2).toInt,x(3).toInt,findPeriod(x(1),x(2),x(3)),x(4).toDouble,x(5),x(6))))
      val joinMap = actualRDD.leftOuterJoin(foreCastRDD).map(f => (f._1,(f._2._1,checkForOption(f._2._2, defaultForeCase))))
      joinMap.foreach(println)
      val dos = joinMap.map(f => dosCal(f._1,f._2._1,f._2._2))
      dos
  }
  
}