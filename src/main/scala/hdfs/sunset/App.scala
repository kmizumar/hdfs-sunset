package hdfs.sunset

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File

object App {
  private val logger: Logger = LogManager.getLogger(this.getClass.getName)

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
      case _ =>
        // arguments are bad, error message will have been displayed
    }
  }
}
