package quant

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class FindRange(lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool]) extends Component {

  val io = new Bundle {
    val x = slave(Flow(Bits(16 bits)))
    val max = master(Flow(Bits(16 bits)))
    val min = master(Flow(Bits(16 bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.x)
  util.AxiStreamSpecRenamer(io.max)
  util.AxiStreamSpecRenamer(io.min)

  val cfg = new Bundle {
    val length = in UInt (16 bits)
  }

  //  inf = 7c00
  // -inf = fc00

  val cnt = UInt(16 bits).setAsReg().init(0)
  val cntAbout2Ovf = cnt === cfg.length - 1
  val cntOvf = Bool().setAsReg().init(False)
  when(io.x.valid) {
    cnt := cnt + 1
    when(cntAbout2Ovf)(cntOvf.set())
    when(cntOvf) {
      cnt.clearAll()
      cntOvf.clear()
    }
  }

  val maxFlow = Flow(Bits(16 bits))
  val minFlow = Flow(Bits(16 bits))

  maxFlow.valid := io.x.valid
  minFlow.valid := io.x.valid
  maxFlow.payload.setAsReg().init(0xfc00)
  minFlow.payload.setAsReg().init(0x7c00)

  val xLessThanMin = lt_func(io.x, minFlow)
  val xGreaterThanMax = lt_func(maxFlow, io.x)

  val minNext = Bits(16 bits)
  val maxNext = Bits(16 bits)

  minNext := minFlow.payload
  maxNext := maxFlow.payload

  when(io.x.valid) {
    when(xLessThanMin.payload) {
      minNext := io.x.payload
    }
    when(xGreaterThanMax.payload) {
      maxNext := io.x.payload
    }
  }

  when(io.x.valid) {
    minFlow.payload := minNext
    maxFlow.payload := maxNext
    when(cntOvf) {
      minFlow.valid.clear()
      maxFlow.valid.clear()
      minFlow.payload := 0x7c00
      maxFlow.payload := 0xfc00
    }
  }

  val vld = io.x.valid & cntOvf
  val vldDly = RegNext(vld, False)

  io.min.valid := vldDly
  io.max.valid := vldDly
  io.min.payload := RegNext(minNext)
  io.max.payload := RegNext(maxNext)
}

object FindRange extends App {
  SpinalVerilog(new FindRange(util.fp16lt0.lt_async))
}