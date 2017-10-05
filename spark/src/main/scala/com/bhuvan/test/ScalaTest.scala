package com.bhuvan.test

object ScalaTest {
  def main(args: Array[String]): Unit = {
    
    val list = List(1,2,3,4,5,6)
    
    
  val b =   list.reduce(_+_)
  println(b)
  
  val c = list.foldRight(-1){
      (x,y)=>(x-y)
    }
    
    /*
     * 
     * 
     * 
     * 
     */
    
    
    
     val d = list.foldLeft(-1){
      (x,y)=>(x-y)
    } 
     
     /*
      * x=-1 y = -1 => (x-y) = -2
      * x=-2 y = 2 => (x-y) = -4
      * x=-4 y = 3 => (x-y) = -7
      * x =-7 y =4 => (x-y) = -11
      * x = -11 y = 5 => (x-y) = -16
      * x = -16 y = -6 => (x-y) = -22
      *
      * 
      */
    
    println(c)
    println(d)
    
    println(Long.MinValue)
    println(Long.MaxValue)
    
  }
}