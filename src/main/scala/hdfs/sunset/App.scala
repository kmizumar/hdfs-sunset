package hdfs.sunset

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.api.java.{JavaRDD, JavaPairRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import java.io.File

object App {
  private val logger: Logger = LogManager.getLogger(this.getClass.getName)

  private val BUFFER_SIZE = 16 * 1024 * 1024  // 16MiB
  private val NUM_PARTITIONS = 100

  case class Config(
    outputDir: File = new File("."),
    dirs: Seq[File] = Seq(),
    numPartitions: Int = NUM_PARTITIONS)

  def main(args: Array[String]): Unit = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("hdfs-sunset"),
        head("HDFS Sunset", "0.0.1"),
        opt[File]('o', "output")
          .required()
          .valueName("<path>")
          .action((x, c) => c.copy(outputDir = x))
          .text("output directory for hash values"),
        opt[Int]('p', "parallel")
          .required()
          .valueName("<number>")
          .action((x, c) => c.copy(numPartitions = x))
          .text("number of parallelism"),
        help("help").text("prints this usage text"),
        arg[File]("<path>...")
          .unbounded()
          .action((x, c) => c.copy(dirs = c.dirs :+ x))
          .text("directories to be scanned"),
        checkConfig(c =>
          if (c.dirs.isEmpty) failure("missing input paths")
          else success)
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        val spark = SparkSession.builder()
          .appName(this.getClass.getName)
          .getOrCreate()
        logger.info("App Name: " + spark.sparkContext.appName)
        logger.info("Deploy Mode: " + spark.sparkContext.deployMode)
        logger.info("Master: " + spark.sparkContext.master)
        startScan(spark, config)
      case _ =>
        // arguments are bad, error message will have been displayed
    }
  }

  def fileToPath(file: File): Path = {
    new Path(s"$file")
  }

  def startScan(spark: SparkSession, config: Config): Unit = {
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val serializableConf = ConfigSerDeser(hadoopConfig)
    val hdfs = FileSystem.get(hadoopConfig)

    val outPath = fileToPath(config.outputDir)
    if (hdfs.exists(outPath)) {
      logger.error(s"output directory already exists: $outPath")
    } else {
      def genPair(fss: Seq[FileStatus]): JavaPairRDD[String, Long] =
        new JavaPairRDD(spark.sparkContext.parallelize(
          fss.filter(_.isFile).map(fs => (fs.getPath.toString, fs.getLen))))

      val fss0: Seq[FileStatus] = hdfs.listStatus(config.dirs.map(fileToPath).toArray)
      var sizePairRdd: JavaPairRDD[String, Long] = genPair(fss0)
      var dirs: Seq[Path] = fss0.filter(fs => fs.isDirectory && !fs.getPath.getName.startsWith(".")).map(_.getPath)

      while (dirs.nonEmpty) {
        val fss: Seq[FileStatus] = hdfs.listStatus(dirs.toArray)
        sizePairRdd = sizePairRdd.union(genPair(fss))
        dirs = fss.filter(fs => fs.isDirectory && !fs.getPath.getName.startsWith(".")).map(_.getPath)
      }

      def fn0 = (x: (String, Long)) => x._2 >= 1024L * 1024L * 1024L * 1024L
      def fn1 = (x: (String, Long)) => x._2
      def fn2 = (x: (String, Long)) => x._1
      val hugeFileRdd = sizePairRdd.filter(fn0)
        .sortBy(fn1, false, config.numPartitions)
        .map(fn2)
        .zipWithIndex()

      val hugeFileMap = spark.sparkContext.broadcast(hugeFileRdd.collect().toMap)

      object SizePairPartitioner extends Partitioner {
        override def numPartitions: Int = config.numPartitions

        override def getPartition(key: Any): Int = {
          val _hugeFileMap: Map[String, Long] = hugeFileMap.value
          key match {
            case (path: String, size: Long) =>
              if (_hugeFileMap.contains(path)) {
                (_hugeFileMap(path) % numPartitions).toInt
              } else {
                (size % numPartitions).toInt
              }
            case path: String =>
              if (_hugeFileMap.contains(path)) {
                (_hugeFileMap(path) % numPartitions).toInt
              } else {
                path.hashCode.abs % numPartitions
              }
            case _ => 0
          }
        }
      }
      case class HashPair(path: String, hash: String)

      sizePairRdd = sizePairRdd.partitionBy(SizePairPartitioner).persist(MEMORY_AND_DISK)
      val pathRdd: RDD[String] = JavaRDD.toRDD(sizePairRdd.map(
        new org.apache.spark.api.java.function.Function[(String, Long), String]() {
          override def call(item: (String, Long)): String = {
            item._1
          }
      }))

      val hashPairRdd: RDD[HashPair] = pathRdd.map(s => {
          import java.security.{DigestInputStream, MessageDigest}
          val hadoopConfig = serializableConf.get()
          val hdfs = FileSystem.get(hadoopConfig)
          val in = hdfs.open(new Path(s))
          val digest = MessageDigest.getInstance("SHA-256")
          try {
            val dis = new DigestInputStream(in, digest)
            val buffer = new Array[Byte](hadoopConfig.getInt("dfs.blocksize", BUFFER_SIZE))
            while (dis.read(buffer) >= 0) {}
            dis.close()
            HashPair(s, digest.digest.map("%02x".format(_)).mkString)
          } finally {
            in.close()
          }
      })
      hashPairRdd.saveAsTextFile(config.outputDir.toString)
    }
  }
}
