package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/3/158:43
* com.theshybigdata
*/
object TestAgain extends App {
  val func3 = (x:Int,y:String) =>(
    x+y
  )
  val func1: PartialFunction[String, Int] = {
    case "one" => 1
    case "two" => 2
    case _ => -1
  }
  println(func1("one"))

  val map = Map("a" -> 1, "b" -> 2)
  val v = map.get("b") match {
    case Some(i) => i
    case None => 0
  }


}
