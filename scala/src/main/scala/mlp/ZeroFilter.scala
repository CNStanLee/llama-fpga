package mlp

import adapter.{FlowGate, TagMap}
import spinal.core._
import spinal.lib._
import util.FlowFragmentAlign

import scala.language.postfixOps

class ZeroFilter(
                  width: Int,
                  numOfPort: Int,
                  tagMap: List[(Int, Int)],
                  tagSeqLenMap: List[(Int, Int)],
                  lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool]
                ) extends Component {

  val io = new Bundle {
    val input = Vec(slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6))), numOfPort)
    val index = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val gtZero = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
  }

  val (inTgM, inErr) = TagMap(io.input, tagMap)
  val input = inTgM.m2sPipe()

  val zero = Flow(Bits(width bits))
  zero.valid.set()
  zero.payload.clearAll()

  val zeroLtIn = lt_func(zero, input.translateWith(input.tdata))

  val gtZeroOut = Flow(Bits(width bits))
  gtZeroOut.valid := input.valid & zeroLtIn.payload
  gtZeroOut.payload := input.tdata

//  io.gtZero.valid := gtZeroOut.valid
//  io.gtZero.tdata := gtZeroOut.payload
//  io.gtZero.tuser := input.tuser

  val seqLenHit = tagSeqLenMap.map(pair => input.tuser === pair._1).asBits()
  val seqLen = MuxOH(seqLenHit, tagSeqLenMap.map(pair => U(pair._2 - 1, 16 bits)))

  val indexOut = Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)))

  val inCnt = UInt(16 bits).setAsReg().init(0)
  val inCntOvf = inCnt === seqLen
  when(input.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  indexOut.valid := gtZeroOut.valid
  indexOut.tdata := inCnt.asBits
  indexOut.tuser := input.tuser
  indexOut.last := input.valid & inCntOvf

  val alignIndex = new FlowFragmentAlign(util.AxiFrame(Bits(16 bits), userBit = 6))
  alignIndex.io.input << indexOut
  alignIndex.io.output.m2sPipe >> io.index

  val alignGtZero = new FlowFragmentAlign(util.AxiFrame(Bits(width bits), userBit = 6))
  val gtZero = Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6)))
  gtZero.valid := gtZeroOut.valid
  gtZero.tdata := gtZeroOut.payload
  gtZero.tuser := input.tuser
  gtZero.last := indexOut.last
  alignGtZero.io.input << gtZero
  alignGtZero.io.output.m2sPipe >> io.gtZero
}

object ZeroFilter extends App {
  SpinalVerilog(new ZeroFilter(16, 1, List((1, 2)), List((1, 2)), util.fp16lt0.lt_async))
}
