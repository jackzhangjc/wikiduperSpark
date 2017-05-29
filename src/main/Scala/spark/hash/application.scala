package spark.hash

//import java.util.logging.Logger

//import java.util.logging.Logger
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.util.Random

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
  val rseed = opt[Int](descr = "rseed", required = false, default = Some(112345))
  val NHASHOUTPUTBITS = opt[Int](descr = "NHASHOUTPUTBITS", required = false, default = Some(30))
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
    val rseed = args.rseed()
    val NHASHOUTPUTBITS = args.NHASHOUTPUTBITS()
    val N = args.N()
    val K = args.K()
    var seeds = ListBuffer[Long]()
    var r = new Random(rseed)
    for (i <- 0 to NHASH - 1) {
      seeds += r.nextLong()
    }
    val sigseed = r.nextLong()
    val hasher = Hasher.create(NHASHOUTPUTBITS, seeds.toList)



    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //val shingles = sc.parallelize(test_data)flatMap(line => line.split("\\t"))
    val shingles = textFile
      .map(line => {
        val line_list = line.split("\\t")
        //line_list[0]
        (line_list(0), line_list(1))
      })
      .flatMap(t => {
        val id = t._1
        val sentence = t._2

        // Minhash vector
        // Initialize the minhash vector
        var MINHASH = ListBuffer.fill(NHASH)(Long.MaxValue)
        //var MINHASH = Long.MaxValue
        // Create shingle.

        // Hash
        var shinglect = 0
        if (sentence.length() >= SHINGLELEN) {

          for (i <- 0 to sentence.length() - SHINGLELEN) {
            val shingle = sentence.substring(i, i + SHINGLELEN)

            // Create the list of hash values
            val hash = hasher.hash(shingle)
            //val hash = shingle.hashCode()
            //println(hash)

            //MINHASH = Math.min(MINHASH, hash)

            // Update minhash signature
            for (j <- 0 to hash.length - 1) {
              MINHASH(j) = Math.min(MINHASH(j), hash(j))
            }
            shinglect += 1
          }

          if (shinglect < MAXLEN && shinglect > MINLEN) {
            val r = new Random(sigseed)
            var list = ListBuffer[(String, (String, String))]()
            for (j <- 0 to N - 1) {
              var signature = ""
              for (i <- 0 to K - 1) {
                val x = r.nextInt(NHASH)
                signature += MINHASH(x).toString()
              }
              println(signature)
              list += ((signature, (id, sentence)))
            }
            //(MINHASH, (id, sentence))
            list.toList
          } else {
            List()
          }
        } else {
          List()
        }
      })
      /*.filter(t => {
        t._1 != 0
      })*/
      .groupByKey()
      .map(t => {
        var hashmap = new HashMap[String, (String, String)]
        t._2.foreach(x => {
          if (!hashmap.contains(x._2)) {
            hashmap.put(x._2, x)
          }
        })
        (t._1, hashmap.keys.toList)
      })
      .filter(t => t._2.size > 1)
      //.collect()
      .saveAsTextFile(output)
      //.foreach(println)

  }
}