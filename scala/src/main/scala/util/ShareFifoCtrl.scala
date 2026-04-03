package util

import spinal.core._
import spinal.lib._

import java.awt.peer.TextAreaPeer
import scala.language.postfixOps

class ShareFifoCtrl[T <: Data](
                                dataType: HardType[T],
                                depth: Int,
                                reuseWidth: Int = 8
                              ) extends Component {

  val io = new Bundle {
    val push = slave Stream dataType
    val pop = master Stream dataType
    val reuse = in UInt (reuseWidth bits)
    val length = in UInt (log2Up(depth) bits)
    val selCycleOnPush = in Bool()
    val selCycleOnPop = in Bool()
  }

  val rdPort = master(MemRdPort(dataType(), depth * 2))
  val wrPort = master(Flow(MemWrPort(dataType(), depth * 2)))

  val cycle = new Area {
    val popPre = Event
    val popPrePipe = popPre.m2sPipe()

    val pushPtr = Counter(depth)
    val popPtr = UInt(log2Up(depth) bits) setAsReg() init 0
    val markPtr = UInt(log2Up(depth) bits) setAsReg() init 0
    val reuseCnt = Counter(reuseWidth bits)
    val lenCnt = Counter(depth)

    val popping = popPre.fire
    val pushing = io.push.fire

    val lenCntEquBound = lenCnt.value === io.length
    val reuseCntEquBound = reuseCnt.value === io.reuse
    val reuseCntEquBoundPre = reuseCnt.value === io.reuse - 1 || io.reuse === 0

    val lenCntEquZero = Bool() setAsReg() init true
    val reuseCntEquZero = Bool() setAsReg() init true
    val phase1 = Bool() setAsReg() init true
    val phase2 = Bool() setAsReg() init false
    val phase3 = Bool() setAsReg() init false

    val popPtrNext = Mux(popPtr === U(depth - 1), U(0), popPtr + 1)

    val risingOccupancy = RegInit(False)
    val pushMatchMark = pushPtr.value === markPtr
    val pushMatchPop = pushPtr.value === popPtr

    val phase1Full = phase1 && pushMatchMark && Mux(lenCntEquZero, risingOccupancy, True)
    val phase2Full = phase2 && pushMatchMark
    val phase3Full = phase3 && pushMatchPop

    val phase1Empty = phase1 && pushMatchPop && Mux(lenCntEquZero, !risingOccupancy, True)
    val phase2Empty = False
    val phase3Empty = False

    val full = phase1Full || phase2Full || phase3Full
    val empty = phase1Empty || phase2Empty || phase3Empty

    //    io.push.ready := !full
    popPre.valid := !empty

    when(pushing =/= popping) {
      risingOccupancy := pushing
    }

    when(pushing) {
      pushPtr.increment()
    }

    when(popping) {
      lenCnt.increment()
      lenCntEquZero.clear()
      popPtr := popPtrNext
      when(lenCntEquBound) {
        reuseCnt.increment()
        lenCnt.clear()
        lenCntEquZero.set()
        reuseCntEquZero.clear()
        popPtr := markPtr

        when(reuseCntEquZero) {
          phase1.clear()
          phase2.set()
        }
        when(reuseCntEquBoundPre) {
          phase2.clear()
          phase3.set()
        }
        when(reuseCntEquBound) {
          phase3.clear()
          phase1.set()

          popPtr := popPtrNext
          markPtr := popPtrNext
          reuseCnt.clear()
          reuseCntEquZero.set()
        }
      }
    }
  }

  val fifo = new Area {
    val popPre = Event
    val popPrePipe = popPre.m2sPipe()

    val popping = popPre.fire
    val pushing = io.push.fire

    val popCntGen = new LoopsCntGen(
      width = List(io.length.getWidth, io.reuse.getWidth)
    )
    popCntGen.io.enable := popping
    popCntGen.io.bound(0) := io.length
    popCntGen.io.bound(1) := io.reuse

    val pushPtr = UInt(log2Up(depth) bits) setAsReg() init 0
    val popPtr = UInt(log2Up(depth) bits) setAsReg() init 0
    val markStart = UInt(log2Up(depth) bits) setAsReg() init 0
    val markEnd = UInt(log2Up(depth) bits) setAsReg() init 0

    val pushPtrNext = UInt(log2Up(depth) bits)
    val pushPtrPlus = pushPtr + 1
    pushPtr := pushPtrNext
    pushPtrNext := pushPtr

    val popPtrNext = UInt(log2Up(depth) bits)
    val popPtrPlus = popPtr + 1
    popPtr := popPtrNext
    popPtrNext := popPtr

    when(pushing) {
      pushPtrNext := pushPtrPlus
    }

    when(popping) {
      popPtrNext := popPtrPlus
      when(popCntGen.io.cntOvf(0)) {
        popPtrNext := markStart
        when(popCntGen.io.cntOvf(1)) {
          popPtrNext := popPtrPlus
        }
      }
    }

    val endInc = popCntGen.io.cnt(1) === 0
    val startInc = popCntGen.io.cntOvf(1)

    when(popping & endInc) {
      markEnd := popPtrPlus
    }

    when(popping & startInc) {
      markStart := popPtrPlus
    }

    val startRising = RegInit(False)
    when(pushing =/= (popping & startInc)) {
      startRising := pushing
    }

    val endRising = RegInit(False)
    when(pushing =/= (popping & endInc)) {
      endRising := pushing
    }

    val pushMatchEnd = pushPtr === markEnd
    val pushMatchStart = pushPtr === markStart

    val full = Bool()
    val empty = Bool()

    full := pushMatchStart && startRising
    empty := pushMatchEnd && !endRising

    //    io.push.ready := !full
    popPre.valid := !empty
  }

  val cycleRdAddr = cycle.popPtr.expand
  val fifoRdAddr = (B"1" ## fifo.popPtr).asUInt

  val cycleWrAddr = cycle.pushPtr.value.expand
  val fifoWrAddr = (B"1" ## fifo.pushPtr).asUInt

  rdPort.cmd.valid := Mux(io.selCycleOnPop, cycle.popPre.ready, fifo.popPre.ready)
  rdPort.cmd.payload := Mux(io.selCycleOnPop, cycleRdAddr, fifoRdAddr)
  io.pop.valid := Mux(io.selCycleOnPop, cycle.popPrePipe.valid, fifo.popPrePipe.valid)
  cycle.popPrePipe.ready := io.pop.ready & io.selCycleOnPop
  fifo.popPrePipe.ready := io.pop.ready & ~io.selCycleOnPop
  io.pop.payload := rdPort.rsp

  wrPort.valid := Mux(io.selCycleOnPush, cycle.pushing, fifo.pushing)
  wrPort.payload.address := Mux(io.selCycleOnPush, cycleWrAddr, fifoWrAddr)
  wrPort.payload.data := io.push.payload
  io.push.ready := ~Mux(io.selCycleOnPush, cycle.full, fifo.full)
}

object ShareFifoCtrl extends App {
  SpinalVerilog(new ShareFifoCtrl(Bits(128 bits), 32))
}