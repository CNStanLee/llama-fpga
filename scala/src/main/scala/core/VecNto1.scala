package core

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class VecNto1(
               width: Int,
               len:Int,
               reduce_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
             ) extends Component {

  val io = new Bundle {
    val inputs = Vec(slave(Flow(Bits(width bits))), len)
    val output = master(Flow(Bits(width bits)))
  }
  val res = io.inputs.reduceBalancedTree(reduce_func)
  io.output << res
}
