package spark.hash

//import java.util.logging.Logger

//import java.util.logging.Logger
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class confApplication(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val NHASH = opt[Int](descr = "number of hash functions", required = false, default = Some(20))
  //val NHASHOUTPUTBITS = opt[Int](descr = "NHASHOUTPUTBITS", required = false, default = Some(30))
  val MINLEN = opt[Int](descr = "MINLEN", required = false, default = Some(75))
  val MAXLEN = opt[Int](descr = "MAXLEN", required = false, default = Some(600))
  val K = opt[Int](descr = "K", required = false, default = Some(10))
  val N = opt[Int](descr = "N", required = false, default = Some(10))
  val SHINGLELEN = opt[Int](descr = "SHINGLELEN", required = false, default = Some(12))
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object Application {
  var log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    val sc = new SparkContext(conf)
    val args = new confApplication(argv)

    log.info("input" + args.input())
    log.info("output" + args.output())
    val textFile = sc.textFile(args.input())
    val output = args.output()
    val NHASH = args.NHASH()
    val SHINGLELEN = args.SHINGLELEN()
    val MAXLEN = args.MAXLEN()
    val MINLEN = args.MINLEN()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //val shingles = sc.parallelize(test_data)flatMap(line => line.split("\\t"))
    val shingles = textFile
      .map(line => {
        val line_list = line.split("\\t")
        //line_list[0]
        (line_list(0), line_list(1))
      })
      .map(t => {
        val id = t._1
        val sentence = t._2

        // Minhash vector
        // Initialize the minhash vector
        //var MINHASH = List.fill(NHASH)(Long.MaxValue)
        var MINHASH = Long.MaxValue
        // Create shingle.

        // Hash
        var shinglect = 0
        if (sentence.length() >= SHINGLELEN) {

          for (i <- 0 to sentence.length() - SHINGLELEN) {
            val shingle = sentence.substring(i, i + SHINGLELEN)

            // Create the list of hash values
            //var hash = List[Long]
            //hash = hashfamily.hash(shingle)
            val hash = shingle.hashCode()
            //println(hash)

            MINHASH = Math.min(MINHASH, hash)

            // Update minhash signature
            /*for (j <- 0 to hash.length()) {
              if (hash(i) < MINHASH(j)) {
                MINHASH(j) = hash(j)
                hashval(j) = shingle
              }
            }*/
            shinglect += 1
          }

          if (shinglect < MAXLEN && shinglect > MINLEN) {
            (MINHASH, (id, sentence))
          } else {
            (0, 0)
          }
        } else {
          (0, 0)
        }
        //val signatures = zdata.flatMap(v => model.hashFunctions.flatMap(h => List(((v._2, h._2 % numBands),h._1.minhash(v._1))))).cache()
      })
      .filter(t => {
        t._1 != 0
      })
      .groupByKey()
      .filter(t => t._2.size > 1)
      //.collect()
      .saveAsTextFile(output)
      //.foreach(println)
    //val rdd = sc.parallelize(data)

  }
}