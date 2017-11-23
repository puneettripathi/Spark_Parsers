import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

// create hiveCtx
var hiveCtx = new HiveContext(sc)

// read textFile
val tabrdd = sc.textFile("PATH_to_TAB_DELIMITED_FILE_ON HDFS")

// filter rdd remove headers
val rdd = tabrdd.map(_.split("\t")).filter(_(0) != "DATECUTOFF").map(a => Row.fromSeq(a))

// header
val header = tabrdd.map(_.split("\t")).filter(_(0) == "DATECUTOFF").first()
val schema = StructType(header.map(fieldName => StructField(fieldName.asInstanceOf[String],StringType,true)))

// Create dataframe
val df = hiveCtx.createDataFrame(rdd,schema)
