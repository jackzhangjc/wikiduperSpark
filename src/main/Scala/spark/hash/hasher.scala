package spark.hash

/**
  * Created by jackzhang on 5/16/17.
  */
import scala.collection.mutable.ListBuffer
import scala.util.Random
//import org.apache.spark.mllib.linalg.SparseVector

/**
    * simple hashing function. defined by ints a, b, p, m
    * where a and b are seeds with a > 0.
    * p is a prime number, >= u (largest item in the universe)
    * m is the number of hash bins
  */

class MultipleShiftHash(m : Int, seeds : List[Long]) extends Serializable {

  private val MAXLENGTH = 10000
  private var acoeffmatrix = Array.ofDim[Long](seeds.length, MAXLENGTH)
  private var bcoeffmatrix = Array.ofDim[Long](seeds.length, MAXLENGTH)

  // Constructor
  for (i <- 0 to seeds.length - 1) {
    val r = new Random(seeds(i))
    for (j <- 0 to MAXLENGTH - 1) {
      acoeffmatrix(i)(j) = r.nextLong()
      bcoeffmatrix(i)(j) = r.nextLong()
    }
  }

  //override def toString(): String = "(" + a + ", " + b + ")";

  def hash(v: List[Long]): List[Long] = {
    //(((a.longValue * x) + b) % p).intValue % m
    var hashvec = ListBuffer[Long]()
    var sum:Long = 0
    for (s <- 0 to seeds.length - 1) {
      for (i <- 0 to Math.min(v.length, MAXLENGTH) - 1) {
        var a = acoeffmatrix(s)(i)
        var b = bcoeffmatrix(s)(i)

        // a has to be odd.
        if (a % 2 == 0) {
          a += 1
        }
        sum += v(i) * a + b
      }
      hashvec += (sum >> (64 - m))
    }
    hashvec.toList
  }

  def hash(s: String) : List[Long] = {
    val b = s.getBytes()
    // Long is 64 bits.
    val longbytes = 64/8
    var v = Array.ofDim[Long](b.length/longbytes + 1)
    for (i <- 0 to b.length - 1) {
      v(i/longbytes) |= ((b(i) & 0xff) << (longbytes*(i%longbytes)))
    }
    hash(v.toList)
  }

  /*def minhash(v: SparseVector): Int = {
    v.indices.map(i => hash(i)).min
  }*/
}

object Hasher {
  /** create a new instance providing p and m. a and b random numbers mod p */
  def create(m : Int, seeds : List[Long]) = new MultipleShiftHash(m, seeds)

}

