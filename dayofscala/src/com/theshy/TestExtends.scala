package com.theshy

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2712:25
* com.theshybigdata
*/
class TestExtends {
  val name = "fulei"
  var id = 111
  def getName:String = {
    name
  }
}

object demo extends TestExtends with App {
    println(name)
    println(getName)


    println(name)
    override def getName:String="父類還是子類"+super.getName
    println(getName)
    override val name = "子類"
}
