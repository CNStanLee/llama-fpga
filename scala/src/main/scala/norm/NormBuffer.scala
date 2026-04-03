package norm

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class NormBuffer(id: Int, width: Int, dim: Int, numOfCore: Int) extends Component {

  val io = new Bundle {
    val input = slave(Flow(Bits(width bits)))
    val output = master(Stream(Bits(width bits)))
    val isAttnLn = in Bool()
    val isMlpLn = in Bool()
  }

  val buf = Mem(Bits(width bits), dim)

  val globalCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val lv1Cnt = UInt(log2Up(dim / numOfCore / numOfCore) bits).setAsReg().init(0)
  val lv2Cnt = UInt(log2Up(numOfCore) bits).setAsReg().init(0)
  val lv3Cnt = UInt(log2Up(numOfCore) bits).setAsReg().init(0)
  val lv1CntOvf = lv1Cnt === dim / numOfCore / numOfCore - 1
  val lv2CntOvf = lv2Cnt === numOfCore - 1
  val lv3CntOvf = lv3Cnt === numOfCore - 1
  when(io.input.valid) {
    lv1Cnt := lv1Cnt + 1
    globalCnt := globalCnt + 1
    when(lv1CntOvf) {
      lv1Cnt.clearAll()
      lv2Cnt := lv2Cnt + 1
      when(lv2CntOvf) {
        lv2Cnt.clearAll()
        lv3Cnt := lv3Cnt + 1
        when(lv3CntOvf) {
          lv3Cnt.clearAll()
          globalCnt.clearAll()
        }
      }
    }
  }

  val seqRW = new Area {

    val enFlag = Bool().setAsReg().init(False)

    val wrVld = io.input.valid & Mux(io.isAttnLn, True, lv3Cnt === id)
    val wrAddr = UInt(log2Up(dim) bits).setAsReg().init(0)
    val wrAddrOvf = wrAddr === Mux(io.isAttnLn, U(dim - 1), U(dim / numOfCore - 1))
    when(wrVld) {
      wrAddr := wrAddr + 1
      when(wrAddrOvf) {
        wrAddr.clearAll()
        enFlag.set()
      }
    }

    val rdEvent = Event
    val rdAddr = UInt(log2Up(dim) bits).setAsReg().init(0)
    val rdAddrOvf = rdAddr === Mux(io.isAttnLn, U(dim - 1), U(dim / numOfCore - 1))
    when(rdEvent.fire) {
      rdAddr := rdAddr + 1
      when(rdAddrOvf) {
        rdAddr.clearAll()
        enFlag.clear()
      }
    }

    rdEvent.valid := enFlag || rdAddr < wrAddr
  }

  val reorder = new Area {

    val enFlag = Bool().setAsReg().init(False)
    enFlag.setWhen(io.input.valid & lv1CntOvf & lv2CntOvf & lv3CntOvf)

    val wrVld = io.input.valid & lv2Cnt === id
    val wrAddr = ((lv1Cnt << log2Up(numOfCore)) + lv3Cnt).resize(log2Up(dim))

    val rdEvent = Event
    val rdAddr = UInt(log2Up(dim / numOfCore) bits).setAsReg().init(0)
    val rdAddrOvf = rdAddr === U(dim / numOfCore - 1)
    when(rdEvent.fire) {
      rdAddr := rdAddr + 1
      when(rdAddrOvf) {
        rdAddr.clearAll()
        enFlag.clear()
      }
    }

    rdEvent.valid := enFlag
  }

  val wrVld = Mux(io.isMlpLn, reorder.wrVld, seqRW.wrVld)
  val wrAddr = Mux(io.isMlpLn, reorder.wrAddr, seqRW.wrAddr)
  buf.write(address = wrAddr, data = io.input.payload, enable = wrVld)

  val rdEvent = StreamMux(io.isMlpLn.asUInt, Vec(seqRW.rdEvent, reorder.rdEvent))
  val rdEventPipe = rdEvent.m2sPipe()
  val rdAddr = Mux(io.isMlpLn, reorder.rdAddr, seqRW.rdAddr)
  io.output.arbitrationFrom(rdEventPipe)
  io.output.payload := buf.readSync(address = rdAddr, enable = rdEvent.ready)
}

object NormBuffer extends App {
  SpinalVerilog(new NormBuffer(0, 16, 4096, 4))
}
