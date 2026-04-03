package core

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Fp32AccEngine(
                     banks: Int,
                     toFp32_func: Flow[Bits] => Flow[Bits],
                     toFp32_func_block: Stream[Bits] => Stream[Bits],
                     toFp16_func: Flow[Bits] => Flow[Bits],
                     fp32Mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                     fp32Add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                     fp32Acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                     toFp32_latency: Int,
                     toFp16_latency: Int,
                     fp32Mul_latency: Int,
                     fp32Add_latency: Int,
                     fp32Acc_latency: Int
                   ) extends Component {

  val io = new Bundle {
    val inputs = Vec(slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)))), banks)
    val postScale = slave(Stream(Bits(32 bits)))
    val output = master(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
  }

  val inputs = Vec(Flow(Bits(16 bits)), banks)
  val inputsFp32 = inputs.map(toFp32_func)
  val reduce = inputsFp32.reduceBalancedTree(fp32Add_func)

  (inputs, io.inputs).zipped.foreach(_.valid := _.valid)
  (inputs, io.inputs).zipped.foreach(_.payload := _.tdata)

  val scaleFp32 = io.postScale

  val scaleFlow = Flow(Bits(32 bits))
  val reduceFlow = Flow(Bits(32 bits))
  val resFlow = fp32Mul_func(scaleFlow, reduceFlow)
  reduceFlow.valid := reduce.valid
  reduceFlow.payload := reduce.payload
  scaleFlow.valid := reduceFlow.valid
  scaleFlow.payload := scaleFp32.payload
  scaleFp32.ready := reduceFlow.valid

  val reduceLatency = fp32Add_latency * log2Up(banks)
  val lastDly = Delay(io.inputs(0).last, toFp32_latency + reduceLatency + fp32Mul_latency, init = False)

  val accIn = Flow(Fragment(Bits(32 bits)))
  accIn.valid := resFlow.valid
  accIn.fragment := resFlow.payload
  accIn.last := lastDly

  val accOut = Flow(Fragment(Bits(32 bits)))
  accOut << fp32Acc_func(accIn)

  val fp32Out = Flow(Bits(32 bits))
  fp32Out.valid := accOut.valid & accOut.last
  fp32Out.payload := accOut.fragment
  val fp16Out = toFp16_func(fp32Out)

  val tuserDly = Delay(io.inputs(0).tuser, toFp32_latency + reduceLatency + fp32Mul_latency + fp32Acc_latency + toFp16_latency, init = B"000000")
  io.output.valid := fp16Out.valid
  io.output.tdata := fp16Out.payload
  io.output.tuser := tuserDly
}

object Fp32AccEngine extends App {
  SpinalVerilog(new Fp32AccEngine(
    banks = 4,

    toFp32_func = util.fp16toFp32.to,
    toFp32_func_block = util.fp16toFp32s.to,
    toFp16_func = util.fp32toFp16.to,
    fp32Mul_func = util.fp32mul8.mul,
    fp32Add_func = util.fp32add11.add,
    fp32Acc_func = util.fp32acc22.acc,

    toFp32_latency = util.fp16toFp32.latency,
    toFp16_latency = util.fp32toFp16.latency,
    fp32Mul_latency = util.fp32mul8.latency,
    fp32Add_latency = util.fp32add11.latency,
    fp32Acc_latency = util.fp32acc22.latency
  ))
}
