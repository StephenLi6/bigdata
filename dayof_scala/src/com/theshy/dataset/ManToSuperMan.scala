package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2720:28
* com.theshybigdata
*/
class Man(val name:String){
  println("i am just a man "+ name)
}

class SuperMan(val name:String){
  def fly {
    print("i am a super man " + name)
  }
}

object ManToSuperMan extends App {
    implicit def man2superman(man:Man) = new SuperMan(man.name)
    private val tom = new Man("tom")
    tom.fly
}


