package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2419:46
* com.theshybigdata
*/
class Person (id:Int,name:String){

  var age:Int =18

  def this(id:Int,name:String,sex:String){
    this(id,name)
    println(id+name+sex)
  }

  private [this] var pet = "小強"

  val names = "super"
  def getNames = this.names

  private val name1="leo"


}


object Person{
  def main(args: Array[String]): Unit = {
    val p = new Person(1,"xiaowang","nan");

  }
}