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
  val NHASHOUTPUTBITS = job.getInt("NHASHOUTPUTBITS", 30)
  val MINLEN = job.getInt("MINLEN", 75)
  MAXLEN = job.getInt("MAXLEN", 600)
  val K = opt[Int](descr = "K", required = false, default = Some(10))
  val N = opt[Int](descr = "N", required = false, default = Some(10))
  val SHINGLELEN = opt[Int](descr = "SHINGLELEN", required = false, default = Some(12))
  //val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object Application {
  var log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val data = List(List(21, 25, 80, 110, 143, 443),
      List(21, 25, 80, 110, 143, 443, 8080),
      List(80, 2121, 3306, 3389, 8080, 8443),
      List(13, 17, 21, 23, 80, 137, 443, 3306, 3389))



    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    val sc = new SparkContext(conf)
    val args = new confApplication(argv)

    log.info("input" + args.input())
    log.info("output" + args.output())
    val textFile = sc.textFile(args.input())
    val NHASH = args.NHASH()
    var SHINGLELEN = args.SHINGLELEN()


    //val output = new Path(args.output)
    val test_data = "Animal nutrition.0000\tbytes = `` 36201 '' > Animal nutrition" +
      " focuses on the dietary needs of domesticated animals , primarily those in agriculture and food production .\n" +
      "Animal nutrition.0001\tIntroduction to animal nutrition There are seven major classes of nutrients : carbohydrates , fats , fiber , minerals , protein , vitamin , and water .\n" +
      "Animal nutrition.0002\tThese nutrient classes can be categorized as either macronutrients -LRB- needed in relatively large amounts -RRB- or micronutrients -LRB- needed in smaller quantities -RRB- .\n" +
      "Animal nutrition.0003\tThe macronutrients are carbohydrates , fats , fiber , proteins , and water .\nAnimal nutrition.0004\tThe micronutrients are minerals and vitamins ."
    //val test_data = "ab"

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
        var MINHASH = List.fill(NHASH)(Long.MaxValue)

        // Create shingle.

        // Hash
        if (sentence.length() >= SHINGLELEN) {
          for (i <- 0 to sentence.length() - SHINGLELEN + 1) {
            val shingle = sentence.substring(i, i + SHINGLELEN)

            // Create the list of hash values
            var hash = List[Long]
            hash = hashfamily.hash(shingle)

            // Update minhash signature
            for (j <- 0 to hash.length()) {
              if (hash(i) < MINHASH(j)) {
                MINHASH(j) = hash(j)
                hashval(j) = shingle
              }
            }
          }
        }
        //val signatures = zdata.flatMap(v => model.hashFunctions.flatMap(h => List(((v._2, h._2 % numBands),h._1.minhash(v._1))))).cache()
      })
      .collect()
      .foreach(println)

    //val rdd = sc.parallelize(data)







  }
}