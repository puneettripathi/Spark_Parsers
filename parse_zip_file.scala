import java.io._
import java.util.zip.ZipInputStream
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

val sqlContext = SQLContext.getOrCreate(sc)

def readZipFile(fileName: String): RDD[String] = {
  log.info(s"reading Zip file ${fileName} to spark RDD")
  val ziprdd = sc.binaryFiles(fileName).flatMap { case (name: String, content: PortableDataStream) =>
    val zis = new ZipInputStream(content.open)
    Stream.continually(zis.getNextEntry)
      .takeWhile(_ != null)
      .flatMap { _ =>
        val br = new BufferedReader(new InputStreamReader(zis))
        Stream.continually(br.readLine()).takeWhile(_ != null)
      }
  }
  ziprdd
}

val rdd = readZipFile('path/to/file')
