package it.ecubecenter.spark.hbase


import java.sql.{Date, Timestamp}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/**
  * Created by gaido on 28/12/2016.
  */
class BulkDelete {
}

object BulkDelete {
  type RowkeyFilterFunction = (Column)=>Column
  case class HBaseColumn (columnFamily:String, columnName:String, columnType:DataType)
  def defaultNamespace = "default"
  def defaultRowkeyType = StringType

  def delete(sqlContext: SQLContext, tableName:String, filterFunction: RowkeyFilterFunction):Unit = delete(sqlContext, defaultNamespace, tableName, filterFunction, defaultRowkeyType, null)
  def delete(sqlContext: SQLContext, tableName:String, filterFunction: RowkeyFilterFunction, additionalColumn:Iterable[HBaseColumn]):Unit = delete(sqlContext, defaultNamespace, tableName, filterFunction, defaultRowkeyType,additionalColumn)
  def delete(sqlContext: SQLContext, namespace:String, tableName:String, filterFunction: RowkeyFilterFunction):Unit = delete(sqlContext, namespace, tableName, filterFunction, defaultRowkeyType, null)
  def delete(sqlContext: SQLContext, tableName:String, filterFunction: RowkeyFilterFunction, rowkeyType:DataType):Unit = delete(sqlContext, defaultNamespace, tableName, filterFunction, rowkeyType, null)
  def delete(sqlContext: SQLContext, namespace:String, tableName:String, filterFunction: RowkeyFilterFunction, rowkeyType:DataType, additionalColumns:Iterable[HBaseColumn]):Unit = {

    val additionalColumnsString = if(additionalColumns != null && additionalColumns.size>0)
      additionalColumns.map( col => s""""${col.columnName}":{"cf":"${col.columnFamily}", "col":"${col.columnName}", "type":"${col.columnType.typeName}"}""").mkString(",")
    else
      ""

    val catalog = s"""{
                      |"table":{"namespace":"${namespace}", "name":"${tableName}"},
                      |"rowkey":"key",
                      |"columns":{
                      |"rowkey":{"cf":"rowkey", "col":"key", "type":"${rowkeyType.typeName}"}
                      |${additionalColumnsString}
                      |}
                      |}""".stripMargin


    val hbaseTable = sqlContext.read.options(Map(HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()
    val keysToDelete = hbaseTable.filter(filterFunction(hbaseTable.col("rowkey")))
    //TODO Complete datatypes
    keysToDelete.foreachPartition(rowKeys => {
      val deletesList = new java.util.LinkedList[Delete]()
      rowkeyType match {
        //case BinaryType => rowKeys.map(_.get)
        case ByteType => rowKeys.foreach( x => deletesList.add(new Delete(Bytes.toBytes(x.getByte(0)))))
        case BooleanType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getBoolean(0)))))
        case DoubleType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getBoolean(0)))))
        case DecimalType() => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getDecimal(0)))))
        case FloatType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getFloat(0)))))
        //case DateType => rowKeys.map(x => new Delete(Bytes.toBytes(x.getDate(0))))
        //case TimestampType => rowKeys.map(x => new Delete(Bytes.toBytes(x.getTimestamp(0))))
        case IntegerType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getInt(0)))))
        case LongType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getLong(0)))))
        case ShortType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getShort(0)))))
        case StringType => rowKeys.foreach(x => deletesList.add(new Delete(Bytes.toBytes(x.getString(0)))))
        case _ => throw new UnsupportedOperationException("Unsupported data type: " + rowkeyType)
      }

      val conf = HBaseConfiguration.create()
      val conn = ConnectionFactory.createConnection(conf)
      val hbaseTable = conn.getTable(TableName.valueOf(namespace,tableName))
      hbaseTable.delete(deletesList)

    })
  }
}

