package mlp

import adapter.FlowGate
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class GreedySampler(
                     width: Int,
                     vocabSize: Int,
                     logitsTag: (Int, Int),
                     lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool]
                   ) extends Component {

  val io = new Bundle {
    val logits = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val argmax = master(Flow(Bits(width bits)))
    val endOfDecode = out Bool()
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.argmax)

  val logitsIn = FlowGate(io.logits, List(logitsTag._1))
  val fp16negInf = 0xfc00

  val maxFlow = Flow(Bits(width bits))
  maxFlow.valid := logitsIn.valid
  maxFlow.payload.setAsReg().init(fp16negInf)

  val cnt = UInt(log2Up(vocabSize) bits).setAsReg().init(0)
  val cntOvf = cnt === vocabSize - 1
  when(logitsIn.valid) {
    cnt := cnt + 1
    when(cntOvf) {
      cnt.clearAll()
    }
  }

  val logitsProbe = Flow(Fragment(Bits(width bits)))
  logitsProbe.valid := logitsIn.valid
  logitsProbe.last := cntOvf
  logitsProbe.payload := logitsIn.payload
  util.AxiStreamSpecRenamer(logitsProbe)

  val xGreaterThanMax = lt_func(maxFlow, logitsIn)

  val index = UInt(log2Up(vocabSize) bits).setAsReg().init(0)
  val indexNext = UInt(log2Up(vocabSize) bits)
  indexNext := index

  val maxNext = Bits(16 bits)
  maxNext := maxFlow.payload

  when(logitsIn.valid) {
    when(xGreaterThanMax.payload) {
      maxNext := logitsIn.payload
      indexNext := cnt
    }
  }

  when(logitsIn.valid) {
    maxFlow.payload := maxNext
    index := indexNext
    when(cntOvf) {
      maxFlow.payload := fp16negInf
      index := 0
    }
  }

  val vld = logitsIn.valid & cntOvf
  val vldDly = RegNext(vld, False)
  val indexDly = RegNext(indexNext.resize(16).asBits)

  val argmax = Flow(Bits(16 bits))
  argmax.valid := vldDly
  argmax.payload := indexDly

  io.argmax << argmax
  //  io.endOfDecode := io.argmax.valid & io.argmax.payload === logitsTag._2
  io.endOfDecode.clear()
}
