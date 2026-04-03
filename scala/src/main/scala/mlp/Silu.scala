package mlp

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Silu(
            exp_latency: Int,
            add_latency: Int,
            exp_func: Flow[Bits] => Flow[Bits],
            div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
            add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
          ) extends Component {

  val io = new Bundle {
    val in = slave(Flow(Bits(16 bits)))
    val out = master(Flow(Bits(16 bits)))
  }

  //  val exp = new Bundle {
  //    val to = master(Flow(Bits(16 bits)))
  //    val from = slave(Flow(Bits(16 bits)))
  //  }
  //  exp.to << neg(io.in)

  def neg(x: Flow[Bits]) = {
    val ret = Flow(Bits(16 bits))
    ret << x
    ret.payload.msb.removeAssignments()
    ret.payload.msb := ~x.payload.msb
    ret
  }

  val negOut = neg(io.in)
  val expOut = exp_func(negOut)

  val fp16One = Flow(Bits(16 bits))
  fp16One.valid.set()
  fp16One.payload := 0x3c00

  val addOut = add_func(fp16One, expOut)

  val inDly = Flow(Bits(16 bits))
  inDly.payload := Delay(io.in.payload, exp_latency + add_latency)
  inDly.valid := addOut.valid

  io.out << div_func(inDly, addOut)
}
