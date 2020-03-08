package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> false
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var i = 0;
    var count = 0;
    while(i < chars.length){
      if(chars(i) == '(') count = count + 1
      else if(chars(i) == ')') count = count - 1

      if(count < 0) return false
      i = i + 1
    }
    if(count != 0) return false else true
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, unmatchedOpening: Int, unmatchedClosing: Int) : (Int, Int) = {
      if(idx >= until) (unmatchedOpening, unmatchedClosing)
      else chars(idx) match {
        case '(' => traverse(idx+1, until, unmatchedOpening + 1, unmatchedClosing)
        case ')' =>
          if(unmatchedOpening != 0) traverse(idx+1, until, unmatchedOpening - 1, unmatchedClosing)
          else traverse(idx+1, until, unmatchedOpening, unmatchedClosing+1)
        case _ => traverse(idx+1, until, unmatchedOpening, unmatchedClosing)
      }
    }

    def reduce(from: Int, until: Int) : (Int,Int) = {

      if(until - from <= threshold) traverse(from, until, 0, 0)
      else{
        val mid = (from + until) / 2
        val ((auo, auc), (buo, buc)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )

        val canMatch = Math.min(auo, buc)
        (auo+buo-canMatch, auc + buc - canMatch)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}