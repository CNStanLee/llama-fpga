package attn

import adapter.{FlowGate, FlowMux, TagMap}
import spinal.core._
import spinal.lib._
import util.Fp16ScaleDown

import scala.language.postfixOps

class SerialSoftmaxFp32(
                         maxSeqLen: Int,
                         numOfPort: Int,
                         softmaxTag: (Int, Int, Int),
                         fp32ToFp16Latency:Int,
                         fp16ToFp32: Flow[Bits] => Flow[Bits],
                         fp32ToFp16: Flow[Bits] => Flow[Bits],
                         lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
                         sub_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                         acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                         div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                         exp_func: Flow[Bits] => Flow[Bits]
                       ) extends Component {

  val io = new Bundle {
    val input = Vec(slave(Flow(util.AxiFrame(Bits(16 bits), userBit = 6))), numOfPort)
    val output = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val seqLen = slave(Stream(Bits(log2Up(maxSeqLen) bits)))
  }

  val softmaxInTag = List(softmaxTag._1, softmaxTag._2)
  val softmaxOutTag = softmaxTag._3

  val filterInput = Vec(io.input.map(f => FlowGate(f, softmaxInTag)))
  val (muxOut, _) = FlowMux(filterInput)
  val input = Flow(Bits(16 bits))
  input := muxOut.m2sPipe

  val softmax = new SoftmaxFp32(32, maxSeqLen, lt_func, sub_func, acc_func, div_func, exp_func)
  softmax.io.input << fp16ToFp32(input)
  softmax.io.seqLen << io.seqLen

  val fp32Out = Flow(Bits(32 bits))
  val fp16Out = fp32ToFp16(fp32Out)
  fp32Out.valid := softmax.io.output.valid
  fp32Out.payload := softmax.io.output.fragment

  io.output.valid := fp16Out.valid
  io.output.tdata := fp16Out.payload
  io.output.tuser := softmaxOutTag
  io.output.last := Delay(softmax.io.output.last, fp32ToFp16Latency)
}
