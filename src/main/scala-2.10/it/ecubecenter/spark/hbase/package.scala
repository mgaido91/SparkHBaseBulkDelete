package it.ecubecenter.spark

import it.ecubecenter.spark.hbase.BulkDelete.{HBaseColumn, RowkeyFilterFunction}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType

/**
  * Created by gaido on 29/12/2016.
  */
package object hbase {
  implicit class HBaseEnhancedSQLContext(sqlContext:SQLContext) {
    def hbaseBulkdelete(tableName:String, filterFunction: RowkeyFilterFunction) = BulkDelete.delete(sqlContext, tableName, filterFunction)
    def hbaseBulkdelete(sqlContext: SQLContext, tableName:String, filterFunction: RowkeyFilterFunction, additionalColumn:Iterable[HBaseColumn]) = BulkDelete.delete(sqlContext, tableName, filterFunction, additionalColumn)
    def hbaseBulkdelete(sqlContext: SQLContext, namespace:String, tableName:String, filterFunction: RowkeyFilterFunction) = BulkDelete.delete(sqlContext, namespace, tableName, filterFunction)
    def hbaseBulkdelete(sqlContext: SQLContext, tableName:String, filterFunction: RowkeyFilterFunction, rowkeyType:DataType) = BulkDelete.delete(sqlContext, tableName, filterFunction, rowkeyType)
    def hbaseBulkdelete(sqlContext: SQLContext, namespace:String, tableName:String, filterFunction: RowkeyFilterFunction, rowkeyType:DataType, additionalColumns:Iterable[HBaseColumn]) =
      BulkDelete.delete(sqlContext, namespace, tableName, filterFunction, rowkeyType, additionalColumns)
  }
}
