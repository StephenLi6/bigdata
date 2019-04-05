package cn.itcast.pyg.tools

import cn.itcast.pyg.bean.Canal
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * HBase的工具类,用来方便我们查询或者存入数据
  */
object HBaseUtils {

  //HBase配置工具
  private val configuration: Configuration = HBaseConfiguration.create()
  //加载application.properties配置文件
  private val config: Config = ConfigFactory.load()

  configuration.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
  configuration.set("hbase.master", config.getString("hbase.master"))
  configuration.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zookeeper.property.clientPort"))
  configuration.set("hbase.rpc.timeout", config.getString("hbase.client.operator.timeout"))
  configuration.set("hbase.client.operator.timeout", config.getString("hbase.client.operator.timeout"))
  configuration.set("hbase.client.scanner.timeout.period", config.getString("hbase.client.scanner.timeout.period"))

  //连接HBase.
  private val connection: Connection = ConnectionFactory.createConnection(configuration)

  private val columnFamily: String = "canal"

  //获取Table对象
  def getTable(tableName: String): Table = {
    val table: TableName = TableName.valueOf(tableName)
    val admin: Admin = connection.getAdmin
    if (!admin.tableExists(table)) {
      val tableDescriptor = new HTableDescriptor(table)
      val columnDescriptor = new HColumnDescriptor(columnFamily)
      tableDescriptor.addFamily(columnDescriptor)
      admin.createTable(tableDescriptor)
    }
    connection.getTable(table)
  }

  def getData(tableName: String, rowKey: String, column: String): String = {
    val table: Table = getTable(tableName)
    val get = new Get(rowKey.getBytes)
    val result: Result = table.get(get)
    val data: Array[Byte] = result.getValue(columnFamily.getBytes, column.getBytes)
    var temp: String = ""
    if (data != null && data.length > 0) {
      temp = Bytes.toString(data)
    }
    temp
  }


  def deleteData(canal: Canal): Any = {

    val table: Table = getTable(canal.tableName)

    if (canal.rowData != null && canal.rowData.size > 0) {
      //rowkey= 数据库名字+表名字+原来的ID
      val rowKey = canal.dbName + ":" + canal.tableName + ":" + canal.rowData(0).columnValue
      val delete = new Delete(rowKey.getBytes())

      table.delete(delete)
    }
  }


  def putData(canal: Canal): Any = {

    val table: Table = getTable(canal.tableName)
    //rowkey= 数据库名字+表名字+原来的ID
    val rowKey = canal.dbName + ":" + canal.tableName + ":" + canal.rowData(0).columnValue
    val put = new Put(rowKey.getBytes())
    for (cell <- canal.rowData) {
      put.addColumn(columnFamily.getBytes(), cell.columnName.getBytes(), cell.columnValue.getBytes())
    }

    table.put(put)
  }
}