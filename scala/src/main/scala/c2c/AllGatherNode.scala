package c2c

import adapter.FlowGate
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class AllGatherNode(id: Int, numOfCore: Int, width: Int, inFifoDepth: Int = 1024 * 16, fromFifoDepth: Int = 1024 * 16) extends Node(id, numOfCore, width) {

  //  require(isPow2(numOfCore))
  //
  //  val idWidth = log2Up(numOfCore)
  //
  //  val io = new Bundle {
  //    val input = slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
  //    val output = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth))))
  //    val from = if (numOfCore != 1) slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth)))) else null
  //    val to = if (numOfCore != 1) master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth)))) else null
  //  }
  //
  //  val singleCore = (numOfCore == 1) generate new Area {
  //    io.output.valid := RegNext(io.input.valid, init = False)
  //    io.output.tdata := RegNext(io.input.payload.tdata)
  //    io.output.tuser := RegNext(io.input.payload.tuser, init = B(0))
  //    io.output.last := RegNext(io.input.payload.last, init = False)
  //    io.output.tdest := 0
  //  }

  val multiCore = (numOfCore > 1) generate new Area {
    val inFifo = StreamFifo(io.input.payload, inFifoDepth, latency = 2, forFMax = true)
    inFifo.io.push.valid := io.input.valid
    inFifo.io.push.payload := io.input.payload

    val fromFifo = StreamFifo(io.from.payload, fromFifoDepth, latency = 2, forFMax = true)
    fromFifo.io.push << io.from.throwWhen(io.from.tdest === id).toStream
    val insert = Stream(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth)))
    insert.arbitrationFrom(inFifo.io.pop)
    insert.tdata := inFifo.io.pop.tdata
    insert.tuser := inFifo.io.pop.tuser
    insert.tdest := id
    insert.last := inFifo.io.pop.last

    val mux = new StreamMux(insert.payload, 2)
    mux.io.inputs(0) << fromFifo.io.pop
    mux.io.inputs(1) << insert

    val muxOutput = mux.io.output.toFlow
    val muxPipe = muxOutput.m2sPipe.m2sPipe
    io.to << muxPipe

    val enCnt = mux.io.output.fire
    io.output.tdest := muxOutput.tdest
    io.output.tdata := muxOutput.tdata
    io.output.tuser := muxOutput.tuser
    io.output.valid := enCnt
    io.output.last := muxOutput.last

    val cnt = UInt(idWidth bits).setAsReg().init(0)
    val insertSel = cnt === 0
    mux.io.select := insertSel.asUInt

    when(enCnt) {
      when(mux.io.output.last) {
        cnt := cnt + 1
        when(cnt === numOfCore - 1) {
          cnt.clearAll()
        }
      }
    }
  }
}

object AllGatherNode extends App {
  SpinalVerilog(new AllGatherNode(0, 1, 16))
}