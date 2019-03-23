package com.theshy

import scala.actors.Actor
//this is a branch test
//todo:第一个例子：利用scala中的actor实现并发编程

class Actor1 extends Actor {
  // 重写act方法
  override def act(): Unit = {
     for(i <- 1 to 10){
       println("Actor1---"+i)
     }
  }
}

class Actor2 extends Actor{
  //重写act方法
  override def act(): Unit = {
    for(j <- 1 to 10 ){
      println("Actor2---"+j)
    }
  }
}

object MyActor{
  def main(args: Array[String]): Unit = {
     //创建actor实例对象
     val actor1 = new Actor1
     val actor2 = new Actor2
    //启动actor
    actor1.start()
    actor2.start()
  }
}
