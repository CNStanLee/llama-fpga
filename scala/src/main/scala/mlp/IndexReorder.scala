package mlp

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class IndexReorder(numOfCore: Int, depth: Int, tag: Int) extends Component {

  val idWidth = log2Up(numOfCore)

  val io = new Bundle {
    val input = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6, destBit = idWidth))))
    val output = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
  }

  val singleCore = numOfCore == 1 generate new Area {
    io.output.valid := RegNext(io.input.valid, init = False)
    io.output.tdata := RegNext(io.input.payload.tdata)
    io.output.tuser := RegNext(io.input.payload.tuser, init = B(0))
    io.output.last := RegNext(io.input.payload.last, init = False)
  }

  val multiCore = numOfCore > 1 generate new Area {
    val fifo = Array.fill(numOfCore)(new StreamFifo(Bits(16 bits), depth, forFMax = true))
    val mux = new StreamMux(Bits(16 bits), numOfCore)
    (mux.io.inputs, fifo).zipped.foreach(_ << _.io.pop)

    val indexLen = Vec(UInt(16 bits), numOfCore)
    val lastSet = Vec(Bool(), numOfCore)
    indexLen.foreach(_.setAsReg().init(0))
    lastSet.foreach(_.setAsReg().init(False))

    for (i <- 0 until numOfCore) {
      fifo(i).io.push.valid := io.input.valid & io.input.tdest === i
      fifo(i).io.push.payload := io.input.tdata

      when(fifo(i).io.push.fire) {
        indexLen(i) := indexLen(i) + 1
        when(io.input.last) {
          lastSet(i).set()
        }
      }
    }

    val outCnt = Vec(UInt(16 bits), numOfCore)
    val outCntOvf = Vec(Bool(), numOfCore)
    outCnt.foreach(_.setAsReg().init(0))

    for (i <- 0 until numOfCore) {
      outCntOvf(i).clear()
      when(fifo(i).io.pop.fire) {
        outCnt(i) := outCnt(i) + 1
        when(lastSet(i) & outCnt(i) === indexLen(i) - 1) {
          outCnt(i) := 0
          outCntOvf(i).set()
          lastSet(i).clear()
          indexLen(i).clearAll()
        }
      }
    }

    val select = UInt(idWidth bits).setAsReg().init(0)
    val selectSwitch = outCntOvf.reduce(_ || _)
    val selectOvf = select === numOfCore - 1
    mux.io.select := select
    when(selectSwitch) {
      select := select + 1
    }

    mux.io.output.ready.set()
    io.output.valid := mux.io.output.valid
    io.output.tdata := mux.io.output.payload
    io.output.tuser := tag
    io.output.last := selectOvf & selectSwitch
  }
}

object IndexReorder extends App {
  SpinalVerilog(new IndexReorder(2, 4096, 6))
}
