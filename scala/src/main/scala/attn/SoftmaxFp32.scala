package attn

import adapter.{FlowGate, FlowMux, TagMap}
import spinal.core._
import spinal.lib._
import util.Fp16ScaleDown

import scala.language.postfixOps

class SoftmaxFp32(
                   width: Int,
                   maxSeqLen: Int,
                   lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
                   sub_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                   div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   exp_func: Flow[Bits] => Flow[Bits]
                 ) extends Component {

  val addrWidth = log2Up(maxSeqLen)
  val fp32Minus16Bits = B(0xc1800000L, width bits)
  val fp32negInf = 0xff800000L

  val io = new Bundle {
    val input = slave(Flow(Bits(width bits)))
    val output = master(Flow(Fragment(Bits(width bits))))
    val seqLen = slave(Stream(Bits(addrWidth bits)))
  }

  val input = io.input

  def local_lt_func(a: Bits, b: Bits): Bool = {
    val aFlow = Flow(Bits(width bits))
    aFlow.valid.set()
    aFlow.payload := a
    val bFlow = Flow(Bits(width bits))
    bFlow.valid.set()
    bFlow.payload := b
    lt_func(aFlow, bFlow).payload
  }

  val inCnt = UInt(addrWidth bits).setAsReg().init(0)
  val inCntZero = inCnt === 0
  val inCntOvf = inCnt === io.seqLen.payload.asUInt
  when(input.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  val maxValFlow = Flow(Bits(width bits))
  maxValFlow.payload.setAsReg().init(fp32negInf)
  maxValFlow.valid.setAsReg().init(False)
  when(input.valid) {
    when(local_lt_func(maxValFlow.payload, input.payload)) {
      maxValFlow.payload := input.payload
    }
    when(inCntOvf) {
      maxValFlow.valid.set()
    }
  }

  val toSubFlow = Flow(Bits(width bits))
  val toExp = sub_func(toSubFlow, maxValFlow)

  val toExpClip = Flow(Bits(width bits))
  toExpClip.valid := toExp.valid
  toExpClip.payload := toExp.payload
  when(local_lt_func(toExp.payload, fp32Minus16Bits)) {
    toExpClip.payload := fp32Minus16Bits
  }

  val toAcc = exp_func(toExpClip)
  val accIn = Flow(Fragment(Bits(width bits)))
  val accOut = acc_func(accIn)

  val accInCnt = UInt(addrWidth bits).setAsReg().init(0)
  val accInCntOvf = accInCnt === io.seqLen.payload.asUInt
  when(accIn.valid) {
    accInCnt := accInCnt + 1
    when(accInCntOvf) {
      accInCnt.clearAll()
    }
  }

  accIn.valid := toAcc.valid
  accIn.payload.fragment := toAcc.payload
  accIn.last := accInCntOvf

  val normFlow = Flow(Bits(width bits))
  normFlow.valid.setAsReg().init(False)
  normFlow.payload.setAsReg().init(0)
  when(accOut.valid & accOut.last) {
    normFlow.valid.set()
    normFlow.payload := accOut.payload.fragment
  }

  val toDivFlow = Flow(Bits(width bits))
  val divOut = div_func(toDivFlow.m2sPipe, normFlow.m2sPipe)

  io.output.valid := divOut.valid
  io.output.fragment := divOut.payload

  io.seqLen.ready.clear()
  val outCnt = UInt(addrWidth bits).setAsReg().init(0)
  val outCntOvf = outCnt === io.seqLen.payload.asUInt

  when(io.output.valid) {
    outCnt := outCnt + 1
    when(outCntOvf) {
      outCnt.clearAll()
      maxValFlow.valid.clear()
      maxValFlow.payload := fp32negInf
      normFlow.clearAll()
      io.seqLen.ready.set()
    }
  }

  io.output.last := outCntOvf

  val subCnt = UInt(addrWidth bits).setAsReg().init(0)
  val subCntOvf = subCnt === io.seqLen.payload.asUInt
  when(toSubFlow.valid) {
    subCnt := subCnt + 1
    when(subCntOvf) {
      subCnt.clearAll()
    }
  }

  val phase0 = Bool().setAsReg().init(True)
  val phase1 = Bool().setAsReg().init(False)
  val phase2 = normFlow.valid
  when(input.valid) {
    when(inCntOvf) {
      phase0 := False
      phase1 := True
    }
  }
  when(toSubFlow.valid) {
    when(subCntOvf) {
      phase1 := False
    }
  }

  val fifo = new StreamFifo(Bits(width bits), maxSeqLen)
  val conflict = input.valid & accIn.valid

  fifo.io.push.valid := input.valid || accIn.valid
  fifo.io.push.payload := Mux(input.valid, input.payload, accIn.payload.fragment)

  toSubFlow.valid := fifo.io.pop.valid & phase1
  toSubFlow.payload := fifo.io.pop.payload

  toDivFlow.valid := fifo.io.pop.valid & phase2
  toDivFlow.payload := fifo.io.pop.payload

  fifo.io.pop.ready := Mux(phase1, phase1, phase2)
}
