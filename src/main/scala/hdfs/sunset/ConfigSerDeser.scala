package hdfs.sunset

import org.apache.hadoop.conf.Configuration

// cf: Use SparkContext hadoop configuration within RDD methods/closures,
// like foreachPartition:
// https://stackoverflow.com/questions/38224132/use-sparkcontext-hadoop-configuration-within-rdd-methods-closures-like-foreachp/39078308
class ConfigSerDeser(var conf: Configuration) extends Serializable {
  def this() {
    this(new Configuration())
  }

  def get(): Configuration = conf

  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}

object ConfigSerDeser {
  def apply(conf: Configuration): ConfigSerDeser = {
    new ConfigSerDeser(conf)
  }
}
