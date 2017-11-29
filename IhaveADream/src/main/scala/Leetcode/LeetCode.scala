package Leetcode

import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode


/**
  * Created by WJ on 2017/11/20.
  */
class LeetCode {
  /**
    * A self-dividing number is a number that is divisible by every digit it contains.

For example, 128 is a self-dividing number because 128 % 1 == 0, 128 % 2 == 0, and 128 % 8 == 0.

Also, a self-dividing number is not allowed to contain the digit zero.

Given a lower and upper number bound, output a list of every possible self dividing number, including the bounds if possible.

Example 1:
Input:
left = 1, right = 22
Output: [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 22]
Note:

The boundaries of each input argument are 1 <= left <= right <= 10000.
    * @param left
    * @param right
    * @return
    */
  def selfDividingNumbers(left: Int, right: Int): List[Int] = {
    val input = List.range(left,right+1)
    import scala.collection.mutable.ListBuffer
    val output = new ListBuffer[Int]
    for (elem <- input) {
      val digits = new ListBuffer[Int]
      if(!elem.toString.contains("0")){
        for(i<-0 until elem.toString.length) {
          digits.append(elem.toString.substring(i,i+1).toInt)
        }
        digits.toList
        var symbol = true
        for (contain <- digits) {
          if(elem%contain!=0) symbol=false
        }
        if(symbol) output.append(elem)
      }
    }
    output.toList
  }

  /**
    * The Hamming distance between two integers is the number of positions at which the corresponding bits are different.

Given two integers x and y, calculate the Hamming distance.

Note:
0 ≤ x, y < 2^31.

Example:

Input: x = 1, y = 4

Output: 2

Explanation:
1   (0 0 0 1)
4   (0 1 0 0)
       ↑   ↑

The above arrows point to positions where the corresponding bits are different.
    * @param x
    * @param y
    * @return
    */
  def hammingDistance(x: Int, y: Int): Int = {
    import scala.math._
   def mkBinary(x:Int):List[Int]={
     import scala.collection.mutable.ListBuffer
     val output = new ListBuffer[Int]
     var temp:Int = x
     while (temp>0) {
       temp%2 match{
         case 0 => output.append(0)
         case 1 => output.append(1)
       }
       temp/=2
     }
     output.toList
   }
    var count:Int = 0
    val input1 = mkBinary(x)
    val input2 = mkBinary(y)
    for(i<-0 until max(input1.length,input2.length)){
      val getElem = (x:Int,input:List[Int])=>if(x<input.length) input(x) else 0
      val elem1 = getElem(i,input1)
      val elem2 = getElem(i,input2)
      if(elem1!=elem2) count+=1
    }
    count
  }

  /**
    *  Initially, there is a Robot at position (0, 0). Given a sequence of its moves, judge if this robot makes a circle, which means it moves back to the original place.

The move sequence is represented by a string. And each move is represent by a character. The valid robot moves are R (Right), L (Left), U (Up) and D (down). The output should be true or false representing whether the robot makes a circle.

Example 1:
Input: "UD"
Output: true
Example 2:
Input: "LL"
Output: false
    * @param moves
    * @return
    */
  def judgeCircle(moves: String): Boolean = {
    val getMoves = moves.toUpperCase()
    if(!getMoves.matches("[RLUD]*")) System.exit(-1)
    var R:Int=0
    var L:Int=0
    var U:Int=0
    var D:Int=0
    for(i<-0 until getMoves.length){
      val x=getMoves.substring(i,i+1)
        x match {
          case "R"=>R+=1
          case "L"=>L+=1
          case "U"=>U+=1
          case "D"=>D+=1
        }
    }
    D==U && L==R
  }

 abstract class TreeNode(x:Int){
   var result:Int=x
   var left:TreeNode
   var right:TreeNode
 }
  def BinaryTree(tree1:TreeNode,tree2:TreeNode):TreeNode={
    if(tree1==null) return tree2
    if(tree2==null) return tree1
    tree1.result+=tree2.result
    tree1.left = BinaryTree(tree1.left,tree2.left)
    tree1.right = BinaryTree(tree1.right,tree2.right)
    tree1
  }

}
object LeetCode{
  def main(args: Array[String]): Unit = {

  }
}
