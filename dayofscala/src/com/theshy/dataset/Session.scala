package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2420:22
* com.theshybigdata
*/
class Session {}
object SessionFactory{
  val session = new Session
  def getSession():Session={
    session
  }
}

object SingletonDemo{
  def main(args: Array[String]): Unit = {
    val session1 = SessionFactory.getSession()
    println(session1)
    val session2 = SessionFactory.session
    println(session2)
  }
}
