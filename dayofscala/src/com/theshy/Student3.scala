package com.theshy

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2519:34
* com.theshybigdata
*/
class Person3{}
class Student3 extends Person3

object Student3 extends App {
    val p:Person3 = new Student3
    val s:Student3 = null
    println(p.isInstanceOf[Person3])
    println(p.getClass)
    println(classOf[Person3])
}
