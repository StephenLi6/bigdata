package cn.itcast.pyg.realprocess.tools

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * Hbase工具类
  */
object HBaseUtils {

  //1.获取连接

  //HBase配置对象
  private val conf: Configuration = HBaseConfiguration.create()
  /*hbase.zookeeper.quorum="node01:2181,node02:2181,node03:2181"
  hbase.master="node01:60000"
  hbase.zookeeper.property.clientPort="2181"
  hbase.rpc.timeout="600000"
  hbase.client.operator.timeout="600000"
  hbase.client.scanner.timeout.period="600000"*/
  //加载application.conf配置文件中的参数
  val config: Config = ConfigFactory.load()
  conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
  conf.set("hbase.master", config.getString("hbase.master"))
  conf.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zookeeper.property.clientPort"))
  conf.set("hbase.rpc.timeout", config.getString("hbase.rpc.timeout"))
  conf.set("hbase.client.operator.timeout", config.getString("hbase.client.operator.timeout"))
  conf.set("hbase.client.scanner.timeout.period", config.getString("hbase.client.scanner.timeout.period"))

  //HBase连接对象
  private val conn: Connection = ConnectionFactory.createConnection(conf)
  //列族名字
  private val familyName: String = "info"
  //有个表

  def getTable(tableName: String): Table = {
    //将字符串表名转换为TableName对象
    val table: TableName = TableName.valueOf(tableName)

    val admin: Admin = conn.getAdmin
    //如果表不存在,创建表
    if (!admin.tableExists(table)) {
      val tableDescriptor = new HTableDescriptor(table)
      val columnDescriptor = new HColumnDescriptor(familyName)
      //将当前表加入列族
      tableDescriptor.addFamily(columnDescriptor)
      admin.createTable(tableDescriptor)
    }
    conn.getTable(table)
  }

  //获取数据

  def getData(tableName: String, rowKey: String, column: String): String = {
    var temp = ""
    val table: Table = getTable(tableName)
    try {
      //获取表
      //创建Get查询对象,指定rowkey
      val get = new Get(rowKey.getBytes())
      val result: Result = table.get(get)
      //通过查询结果对象,获取数据
      val bytes: Array[Byte] = result.getValue(familyName.getBytes(), column.getBytes())

      if (bytes != null && bytes.length > 0) {
        //有数据
        temp = Bytes.toString(bytes)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //记得用完之后释放资源
      table.close()
    }
    temp

  }

  //插入数据
  def putData(tableName: String, rowKey: String, column: String, data: String) = {
    val table: Table = getTable(tableName)
    try {
      val put = new Put(rowKey.getBytes())
      put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes())
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }

  }

  //插入多列数据
  def putMapData(tableName: String, rowKey: String, map: Map[String, String]) = {
    val table: Table = getTable(tableName)
    try {
      val put = new Put(rowKey.getBytes())

      for ((column,data) <- map) {
        put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes())
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }



  def main(args: Array[String]): Unit = {
//        putData("demo", "002", "password", "123456")

    var map = Map[String, String]()

    map += ("username" -> "lisi")
    map += ("password" -> "123")
    map += ("telephone" -> "124353453455")

    putMapData("demo", "003", map)

    val str: String = getData("demo", "002", "username")
    println(str)

  }


}
