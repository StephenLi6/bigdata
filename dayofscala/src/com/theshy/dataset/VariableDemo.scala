package com.theshy.dataset

import scala.collection.mutable.ArrayBuffer

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2420:51
* com.theshybigdata
*/
object VariableDemo extends App {
  var i = 1
  var s = "hello"
  val str:String = "itcast"

  val x = 1
  val y = if(x>0) 1 else -1
  println(y)

  val z = if(x>1) 1 else "error"
  println(z)

  val m = if (x >2) 1
  println(m)

  val k = if(x <0) 0 else if (x>2) 2 else 1
  println(k)

  for(i <- 1 to 3;j <- 1 to 3 if i!=j)
    println(i + j)

  val v = for(i <- 1 to 10) yield i *10
  println(v)

  def m1(x:Int,y:Int):Int=x*y
  val f1 = (x:Int,y:Int) => x+y
  println(f1(1,2))

  val map1 = Map

  val mapBuffer = new ArrayBuffer[Int]()
  mapBuffer.append(5)
  println(mapBuffer)
  private val array: Array[Int] = Array(1,2,3)

  def m(x:Int)= {
    x*x
  }
  val myarray = Array(1,2,3,4).map(m(_))
  println(myarray.mkString(","))

  val function = (x:Int,y:Int) =>{
  x+y
  }

  val function1 : Int =>Int ={
    x=>x*x
  }

  val function2:(Int,String)=>(Int,String)={
    (x,y)=>(x,y)
  }

  println((x:Int,y:Int)=>x+y)

    val a = Array("hello","world")
  val b = Array("Hello","World")
  println(a.corresponds(b)(_.equalsIgnoreCase(_)))
}
