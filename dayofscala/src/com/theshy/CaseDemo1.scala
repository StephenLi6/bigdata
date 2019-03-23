package com.theshy

import scala.util.Random

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2615:51
* com.theshybigdata
*/
object CaseDemo1 extends App {
  val arr = Array("hadoop","zookeeper","spark")
  val name = arr(Random.nextInt(arr.length))
  name match{
    case "hadoop" => println("hadoop")
    case _ => println("i can't recongnize you")
  }

  val arr1 = Array(2.0,2.0,2.0,2.0)
  val r = arr1(Random.nextInt(4))
  r match{
    //case x :Int => println("Int"+x)
    case y :Double if (y<0) => println("Double"+y)
    case _ => println("not match exception")
  }


  val lst = List(3, -1)
  lst match {
    case 0 :: Nil => println("only 0")
    case x :: y :: Nil => println(""+x+y)//println(s"x: $x y: $y")
    case 0 :: tail => println("0 ...")
    case _ => println("something else")
  }

  val map=Map(("a",1),("b",2))
   /*map.get("a") match {
    case Some(i) => i
    case None => 0
  }*/

  println(map("a") +"-----" +map.get("a"))


}
