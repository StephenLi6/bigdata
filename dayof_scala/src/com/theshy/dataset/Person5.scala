package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2520:38
* com.theshybigdata
*/
class Person5 {}
class Student5 extends Person5

object Person5 extends App{
  val p:Person5 = new Student5
  println(p)
  p match {
    case a:Person5 => println("this is a person5"+a)
    case _ => println("unkonwn type1！")
  }
}
