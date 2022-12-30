package hdfs.sunset

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File

object App {
  private val logger: Logger = LogManager.getLogger(this.getClass.getName)

  private val BUFFER_SIZE = 16 * 1024 * 1024  // 16MiB
  private val NUM_PARTITIONS = 100

  case class Config(
    outputDir: File = new File("."),
    dirs: Seq[File] = Seq())

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
      val fss0: Seq[FileStatus] = hdfs.listStatus(config.dirs.map(fileToPath).toArray)
      var fRdd: RDD[String] = spark.sparkContext
        .parallelize(fss0.filter(_.isFile).map(_.getPath.toString))
      var dirs: Seq[Path] = fss0.filter(fs => fs.isDirectory && !fs.getPath.getName.startsWith(".")).map(_.getPath)
      while (dirs.nonEmpty) {
        val fss: Seq[FileStatus] = hdfs.listStatus(dirs.toArray)
        fRdd = fRdd.union(spark.sparkContext
          .parallelize(fss.filter(_.isFile).map(_.getPath.toString)))
        dirs = fss.filter(fs => fs.isDirectory && !fs.getPath.getName.startsWith(".")).map(_.getPath)
      }

      case class HashPair(
        path: String,
        hash: String)

      val hashPairRdd: RDD[HashPair] = fRdd.repartition(NUM_PARTITIONS).map(s => {
        import java.security.{DigestInputStream, MessageDigest}
        val hdfs = FileSystem.get(serializableConf.get())
        val in = hdfs.open(new Path(s))
        val digest = MessageDigest.getInstance("SHA-256")
        try {
          val dis = new DigestInputStream(in, digest)
          val buffer = new Array[Byte](BUFFER_SIZE)
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
