package mlp

import adapter.FlowGate
import spinal.core._
import spinal.lib._
import util.{FlowFragmentAlign, FlowThrowLast, URAM16x16384Fifo}

import scala.language.postfixOps

class UGMul(
             mlpDim: Int,
             width: Int,
             index2UgTag: (Int, Int, Int),
             ugTag: (Int, Int, Int),
             actLatency: Int,
             siluLatency: Int,
             lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
             act_func: Flow[Bits] => Flow[Bits],
             mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
           ) extends Component {

  val fp16SiLUThresholdBits = B(0xc900, 16 bits)
  val fatReluBits = B(0x211f, 16 bits)

  val io = new Bundle {
    val predIndexIn = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val gateIndexOut = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val ugIndexOut = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val allReduceOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val ugOut = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
  }

  val status = new Bundle {
    val enPredictor = in Bool()
    val enFatRelu = in Bool()
  }

  val silu = new Bundle {
    val to = master(Flow(Bits(width bits)))
    val from = slave(Flow(Bits(width bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.ugOut)

  val fifo = new URAM16x16384Fifo()

  val indexIn = FlowGate(FlowThrowLast(io.predIndexIn), List(index2UgTag._1))
  val indexCnt = UInt(16 bits).setAsReg().init(0)
  val indexCntLock = UInt(16 bits).setAsReg().init(0)
  when(indexIn.valid) {
    indexCnt := indexCnt + 1
    when(indexIn.valid & io.predIndexIn.last) {
      indexCntLock := indexCnt
      indexCnt.clearAll()
    }
  }

  val gateIn = FlowGate(io.allReduceOut, List(ugTag._2))
  val gateCnt = UInt(16 bits).setAsReg().init(0)
  val gateCntOvf = gateCnt === Mux(status.enPredictor, indexCntLock, U(mlpDim - 1))
  when(gateIn.valid) {
    gateCnt := gateCnt + 1
    when(gateCntOvf) {
      gateCnt.clearAll()
      indexCntLock.clearAll()
    }
  }

  util.AxiStreamSpecRenamer(gateIn)

  val threshold = Flow(Bits(width bits))
  threshold.valid.set()
  threshold.payload := fatReluBits

  val cmpConstant = threshold
  val cmpRes = lt_func(cmpConstant, gateIn)
  val zeroLtIn = Flow(Bool())

  zeroLtIn.valid := gateIn.valid
  zeroLtIn.payload := Mux(status.enFatRelu, cmpRes.payload, True)

  val indexOut = Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)))
  indexOut.valid := gateIn.valid & zeroLtIn.payload
  indexOut.tdata := Mux(status.enPredictor, fifo.io.pop.fragment, gateCnt.asBits)
  indexOut.tuser := index2UgTag._2
  indexOut.last := gateIn.valid & gateCntOvf

  val indexAlign = new FlowFragmentAlign(util.AxiFrame(Bits(16 bits), userBit = 6))
  indexAlign.io.input << indexOut.m2sPipe
  indexAlign.io.output >> io.gateIndexOut

  val gateFilterAlign = new FlowFragmentAlign(Bits(16 bits))
  gateFilterAlign.io.input.valid := gateIn.valid & (zeroLtIn.payload)
  gateFilterAlign.io.input.fragment := gateIn.payload
  gateFilterAlign.io.input.last := gateIn.valid & gateCntOvf

  val gateFilter = Flow(Bits(width bits))
  gateFilter.valid := gateFilterAlign.io.output.valid
  gateFilter.payload := gateFilterAlign.io.output.fragment

  silu.to << gateFilter

  val reluOut = act_func(gateFilter)
  val actOut = Mux(status.enFatRelu, reluOut, silu.from)

  val reluOutLast = Delay(gateFilterAlign.io.output.last, actLatency, init = False)
  val siluOutLast = Delay(gateFilterAlign.io.output.last, siluLatency, init = False)
  val actOutLast = Mux(status.enFatRelu, reluOutLast, siluOutLast)

  val u = FlowGate(io.allReduceOut, List(ugTag._1))
  val g = Flow(Bits(width bits))
  val ug = mul_func(u, g)


  util.AxiStreamSpecRenamer(u)
  util.AxiStreamSpecRenamer(g)

  g.valid := u.valid
  g.payload := fifo.io.pop.fragment

  //  fifo.io.push.valid := actOut.valid || indexIn.valid
  //  fifo.io.push.last := actOut.valid & actOutLast || indexIn.valid & io.predIndexIn.last
  //  fifo.io.push.fragment := MuxOH(
  //    Vec(actOut.valid, indexIn.valid),
  //    Vec(actOut.payload, indexIn.payload)
  //  )
  fifo.io.push.valid := RegNext(actOut.valid || indexIn.valid, init = False)
  fifo.io.push.last := RegNext(actOut.valid & actOutLast || indexIn.valid & io.predIndexIn.last, init = False)
  fifo.io.push.fragment := RegNext(MuxOH(
    Vec(actOut.valid, indexIn.valid),
    Vec(actOut.payload, indexIn.payload)
  ))

  fifo.io.pop.ready := u.valid || status.enPredictor & gateIn.valid

  io.ugOut.valid := ug.valid
  io.ugOut.tdata := ug.payload
  io.ugOut.tuser := ugTag._3

  //  val ugIndexFifo = new StreamFifo(Bits(width bits), fifoDepth, forFMax = true)
  val ugIndexFifo = new URAM16x16384Fifo()
  val ugLastIndexLock = Bits(16 bits).setAsReg().init(0xffff)
  when(io.gateIndexOut.valid & io.gateIndexOut.last)(ugLastIndexLock := io.gateIndexOut.tdata)
  when(io.ugIndexOut.valid & io.ugIndexOut.last)(ugLastIndexLock.setAll())

  //  ugIndexFifo.io.push.valid := io.gateIndexOut.valid
  //  ugIndexFifo.io.push.fragment := io.gateIndexOut.tdata
  //  ugIndexFifo.io.push.last := io.gateIndexOut.last
  ugIndexFifo.io.pop.ready := ug.valid

  ugIndexFifo.io.push.valid := RegNext(io.gateIndexOut.valid, init = False)
  ugIndexFifo.io.push.fragment := RegNext(io.gateIndexOut.tdata)
  ugIndexFifo.io.push.last := RegNext(io.gateIndexOut.last, init = False)

  val hit = ugLastIndexLock === ugIndexFifo.io.pop.fragment
  io.ugIndexOut.valid := ugIndexFifo.io.pop.fire
  io.ugIndexOut.tdata := ugIndexFifo.io.pop.fragment
  io.ugIndexOut.tuser := index2UgTag._3
  io.ugIndexOut.last := hit

  io.ugOut.last := hit

  //  val io = new Bundle {
  //    val filterOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  //    val allReduceOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  //    val ugOut = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  //  }
  //
  //  val filterOut = FlowGate(io.filterOut, List(ugTag._2))
  //  val allReduceOut = FlowGate(io.allReduceOut, List(ugTag._1))
  //
  //  val actOut = act_func(filterOut)
  //
  //  val gateFifo = new StreamFifo(Bits(width bits), fifoDepth)
  //  gateFifo.io.push.valid := actOut.valid
  //  gateFifo.io.push.payload := actOut.payload
  //
  //  val u = Flow(Bits(width bits))
  //  val g = Flow(Bits(width bits))
  //  val ug = mul_func(u, g)
  //
  //  u.valid := allReduceOut.valid
  //  u.payload := allReduceOut.payload
  //  g.valid := u.valid
  //  g.payload := gateFifo.io.pop.payload
  //  gateFifo.io.pop.ready := g.valid
  //
  //  io.ugOut.valid := ug.valid
  //  io.ugOut.tdata := ug.payload
  //  io.ugOut.tuser := ugTag._3
}
