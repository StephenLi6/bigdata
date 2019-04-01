package com.theshy.dataset

/*
* The Best Or Nothing
* Desinger:TheShy
* Date:2019/2/2720:45
* com.theshybigdata
*/
class Boss {
  def payMoney(implicit name: String) {
    print("我有，," + name)
  }

  def payName(implicit money: Int): Unit = {
    print("wode money = " + money)
  }
}
  object myImpli{
    implicit val name = "zhangsan"
    implicit val  money = 11
  }

  object Boss extends App {
    import myImpli._
    val boss = new Boss
    boss.payMoney
    boss.payName
  }
