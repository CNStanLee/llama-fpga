package util

import adapter.FlowMux
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class ExpFunc(
               port: Int,
               latency: Int,
               lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
               exp_func: Flow[Bits] => Flow[Bits]
             ) extends Component {

  val width = 16
  val fp16Minus16 = 0xcc00
  val fp16Minus16Bits = B(fp16Minus16, width bits)

  val io = new Bundle {
    val inputs = Vec(slave(Flow(Bits(width bits))), port)
    val outputs = Vec(master(Flow(Bits(width bits))), port)
  }

  def local_lt_func(a: Bits, b: Bits): Bool = {
    val aFlow = Flow(Bits(width bits))
    aFlow.valid.set()
    aFlow.payload := a
    val bFlow = Flow(Bits(width bits))
    bFlow.valid.set()
    bFlow.payload := b
    lt_func(aFlow, bFlow).payload
  }

  val inputVld = io.inputs.map(_.valid)
  val inputVldDly = inputVld.map(s => Delay(s, latency + 1, init = False))

  val (toExpMux,toExpErr) = FlowMux(io.inputs)
  val toExp = toExpMux.m2sPipe()

  val toExpClip = Flow(Bits(width bits))
  toExpClip.valid := toExp.valid
  toExpClip.payload := toExp.payload
  when(local_lt_func(toExp.payload, fp16Minus16Bits)) {
    toExpClip.payload := fp16Minus16Bits
  }

  val expOut = exp_func(toExpClip)

  (inputVldDly, io.outputs).zipped.foreach((vld, out) => out.valid := vld)
  io.outputs.foreach(_.payload := expOut.payload)
}
