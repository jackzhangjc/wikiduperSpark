package spark.hash

//import java.util.logging.Logger

import java.util.logging.Logger
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class confApplication(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object Application {
  var log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new confApplication(argv)

    log.info("input" + args.input)
    log.info("output" + args.output)
  }
}