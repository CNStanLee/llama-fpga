package mlp

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class SiluFp32(
            exp_latency: Int,
            add_latency: Int,
            exp_func: Flow[Bits] => Flow[Bits],
            toFp32_func: Flow[Bits] => Flow[Bits],
            toFP16_func: Flow[Bits] => Flow[Bits],
            div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
            add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
          ) extends Component {

  val io = new Bundle {
    val in = slave(Flow(Bits(16 bits)))
    val out = master(Flow(Bits(16 bits)))
  }

  def neg(x: Flow[Bits]) = {
    val ret = Flow(Bits(32 bits))
    ret << x
    ret.payload.msb.removeAssignments()
    ret.payload.msb := ~x.payload.msb
    ret
  }

  val inFp32 = toFp32_func(io.in)
  val expOut = exp_func(neg(inFp32))

  val fp32One = Flow(Bits(32 bits))
  fp32One.valid.set()
  fp32One.payload := 0x3f800000

  val addOut = add_func(fp32One, expOut)

  val inDly = Flow(Bits(32 bits))
  inDly.payload := Delay(inFp32.payload, exp_latency + add_latency)
  inDly.valid := addOut.valid

  val fp32Out = div_func(inDly, addOut)
  val fp16Out = toFP16_func(fp32Out)
  io.out << fp16Out
}
