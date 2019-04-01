package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2712:35
* com.theshybigdata
*/
class TestExtendsObj {
  val name = "fulei"
  var id = 111
  def getName:String = {
    name
  }
}

class Test extends TestExtends{
  override val name = "zilei"
  override def getName: String = super.getName
}

object Test extends App {
  private val test: Test = new Test
  println(test.getName)
}
