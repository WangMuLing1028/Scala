package cn.sibat.bus.utils

import java.sql._
import java.util.Properties

import cn.sibat.bus.{LineCheckPoint, StationData}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types._

import scala.Array
import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * 改造spark的jdbc写数据库的方法
  * 由于spark的jdbc是针对DataFrame操作的
  * 所以在DataFrame计算中无法调用写数据库方法，所以推出DataFrame
  * 在计算过程中随意调用写数据的方法，利用反射的原理把对Row操作的方法进行
  * 改造
  * Created by kong on 2017/8/21.
  */
object DAOUtil {
  /**
    * 把class转成structField格式
    *
    * @param classType class
    * @return
    */
  def toStructFields(classType: AnyRef): Array[StructField] = {
    classType.getClass.getDeclaredFields.map(f => {
      val name = f.getGenericType.getTypeName
      val dataTypes = name.substring(name.lastIndexOf(".") + 1).toLowerCase match {
        case "string" => DataTypes.StringType
        case "long" => DataTypes.LongType
        case "float" => DataTypes.FloatType
        case "boolean" => DataTypes.BooleanType
        case "date" => DataTypes.DateType
        case "double" => DataTypes.DoubleType
        case "int" => DataTypes.IntegerType
        case "integer" => DataTypes.IntegerType
        case "timestamp" => DataTypes.TimestampType
        case "byte" => DataTypes.ByteType
        case "binary" => DataTypes.BinaryType
        case "short" => DataTypes.ShortType
        case _ => DataTypes.NullType
      }
      StructField(f.getName, dataTypes)
    })
  }

  /**
    * structFields转成StructType
    *
    * @param structFields structFields
    * @return
    */
  def toStructType(structFields: Array[StructField]): StructType = {
    StructType.apply(structFields)
  }

  /**
    * 把class转成structType格式
    *
    * @param classType class
    * @return
    */
  def toStructType(classType: AnyRef): StructType = {
    StructType.apply(toStructFields(classType))
  }

  /**
    * 插入数据的列表示
    *
    * @param colName 列名
    * @return
    */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /**
    * 生成插入语句
    *
    * @param conn      连接
    * @param table     表名
    * @param rddSchema 构造
    *                  Returns a PreparedStatement that inserts a row into table via conn.
    */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType)
  : PreparedStatement = {
    val columns = rddSchema.fields.map(x => x.name).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
    * 查询station点
    *
    * @param url    数据库的url
    * @param lineId 线路id
    * @return
    */
  def selectStationStatement(url: String, lineId: String): mutable.Map[String, Array[StationData]] = {
    val checkpointMap = new mutable.HashMap[String, Array[StationData]]()
    val conn = JdbcUtils.createConnectionFactory(url, new Properties())()
    val sql = s"select l.ref_id as route,l.direction as direct,s.station_id as stationId,ss.name as stationName,s.stop_order as stationSeqId,ss.lat as stationLat,ss.lon as stationLon,l.name as lineName from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id and l.ref_id='$lineId'"
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      try {
        while (rs.next()) {
          val route = rs.getString(1)
          val direct = rs.getString(2)
          val stationId = rs.getString(3)
          val stationName = rs.getString(4)
          val stationSeqId = rs.getInt(5)
          val stationLon = rs.getString(6)
          val stationLat = rs.getString(7)
          val lineName = rs.getString(8)
          val sd = StationData(route, lineName, direct, stationId, stationName, stationSeqId, stationLon.toDouble, stationLat.toDouble, 0)
          checkpointMap.update(lineId + "," + direct, checkpointMap.getOrElse(lineId + "," + direct, Array[StationData]()) ++ Array(sd))
        }
      } catch {
        case e: Exception =>
      } finally {
        rs.close()
      }
    } catch {
      case e: Exception =>
    } finally {
      stmt.close()
      conn.close()
    }
    checkpointMap
  }

  /**
    * 查询checkpoint点
    *
    * @param url    数据库的url
    * @param lineId 线路id
    * @return
    */
  def selectCheckpointStatement(url: String, lineId: String): mutable.Map[String, Array[LineCheckPoint]] = {
    val checkpointMap = new mutable.HashMap[String, Array[LineCheckPoint]]()
    val conn = JdbcUtils.createConnectionFactory(url, new Properties())()
    val sql = s"select c.lon as lon,c.lat as lat,l.ref_id as lineId,c.point_order as point_order,l.direction as direct from line_checkpoint c,line l where c.line_id = l.id and l.ref_id='$lineId'"
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      try {
        while (rs.next()) {
          val lon = rs.getString(1)
          val lat = rs.getString(2)
          val lineId = rs.getString(3)
          val point_order = rs.getInt(4)
          val direct = rs.getString(5)
          val lc = LineCheckPoint(lon.toDouble, lat.toDouble, lineId, point_order, direct)
          checkpointMap.update(lineId + "," + direct, checkpointMap.getOrElse(lineId + "," + direct, Array[LineCheckPoint]()) ++ Array(lc))
        }
      } finally {
        rs.close()
      }
    } catch {
      case e: Exception =>
    } finally {
      stmt.close()
      conn.close()
    }
    checkpointMap
  }

  /**
    * 批量写进数据库的方法
    * 适配任何数据库和表结构
    *
    * @param url        数据库的url
    * @param table      表名
    * @param batchSize  批量插入的大小，默认1000
    * @param insertList 待插入的数据
    * @param classType  表类型抽象成的类的类型，用的时候直接传一个实例化对象就可以
    * @return
    */
  def writeToDataBase(url: String, table: String, insertList: Iterator[AnyRef], classType: AnyRef, batchSize: Int = 1000): Int = {
    val conn = JdbcUtils.createConnectionFactory(url, new Properties())()
    //把类抽象成schema
    val schema = toStructType(classType)
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData.supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData.supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        true
    }
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      //生成插入语句
      val stmt = insertStatement(conn, table, schema)
      try {
        var rowCount = 0
        while (insertList.hasNext) {
          val row = insertList.next()
          val numFields = schema.fields.length
          var i = 0
          while (i < numFields) {
            if (row == null) {
              stmt.setNull(i + 1, 0)
            } else {
              schema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, invoke(row, schema.fields(i).name).toInt)
                case LongType => stmt.setLong(i + 1, invoke(row, schema.fields(i).name).toLong)
                case DoubleType => stmt.setDouble(i + 1, invoke(row, schema.fields(i).name).toDouble)
                case FloatType => stmt.setFloat(i + 1, invoke(row, schema.fields(i).name).toFloat)
                case ShortType => stmt.setInt(i + 1, invoke(row, schema.fields(i).name).toShort)
                case ByteType => stmt.setInt(i + 1, invoke(row, schema.fields(i).name).toByte)
                case BooleanType => stmt.setBoolean(i + 1, invoke(row, schema.fields(i).name).toBoolean)
                case StringType => stmt.setString(i + 1, invoke(row, schema.fields(i).name))
                case BinaryType => stmt.setBytes(i + 1, invoke(row, schema.fields(i).name).getBytes)
                case TimestampType => stmt.setTimestamp(i + 1, row.getClass.getDeclaredMethod(schema.fields(i).name).invoke(row).asInstanceOf[Timestamp])
                case DateType => stmt.setDate(i + 1, new Date(row.getClass.getDeclaredMethod(schema.fields(i).name).invoke(row).asInstanceOf[java.util.Date].getTime))
                case ArrayType(et, _) =>
                  // remove type length parameters from end of type name
                  val typeName = getJdbcType(et).databaseTypeDefinition
                    .toLowerCase.split("\\(")(0)
                  val array = conn.createArrayOf(
                    typeName,
                    row.getClass.getDeclaredMethod(schema.fields(i).name).invoke(row).asInstanceOf[Seq[AnyRef]].toArray)
                  stmt.setArray(i + 1, array)
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception =>
        }
      }
    }
    0
  }

  /**
    * 反射类的成员的值
    *
    * @param classType class
    * @param name      某个属性
    * @return
    */
  def invoke(classType: AnyRef, name: String): String = {
    classType.getClass.getDeclaredMethod(name).invoke(classType).toString
  }

  /**
    * 标准的数据库类型
    * Retrieve standard jdbc types.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The default JdbcType for this DataType
    */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
    * Retrieve the jdbc / sql type for a given datatype.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The new JdbcType if there is an override for this DataType
    */
  def getJDBCType(dt: DataType): Option[JdbcType] = None

  /**
    * 获取jdbc的数据类型
    *
    * @param dt dataType
    * @return
    */
  private def getJdbcType(dt: DataType): JdbcType = {
    getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * 把数据写到数据库所传入的tableName中
    *
    * @param df        df
    * @param tableName 表名
    */
  def write2SQL(df: DataFrame, tableName: String, url: String): Unit = {
    df.write.mode("append").jdbc(url, tableName, new Properties())
  }
}
