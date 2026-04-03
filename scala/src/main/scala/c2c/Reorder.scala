package c2c

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Reorder(numOfCore: Int, depth: Int) extends Component {

  val idWidth = log2Up(numOfCore)

  val io = new Bundle {
    val input = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6, destBit = idWidth))))
    val output = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6, destBit = idWidth))))
  }

  val fifo = Array.fill(numOfCore)(new StreamFifo(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)), depth, forFMax = true))
  val mux = new StreamMux(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)), numOfCore)
  (mux.io.inputs, fifo).zipped.foreach(_ << _.io.pop)

  for (i <- 0 until numOfCore) {
    fifo(i).io.push.valid := io.input.valid & io.input.tdest === i
    fifo(i).io.push.payload := io.input.payload
  }

  val select = UInt(idWidth bits).setAsReg().init(0)
  mux.io.select := select
  when(mux.io.output.fire) {
    select := select + 1
  }

  mux.io.output.freeRun()
  io.output.valid := mux.io.output.valid
  io.output.tdata := mux.io.output.tdata
  io.output.tuser := mux.io.output.tuser
  io.output.last := mux.io.output.last
  io.output.tdest := select.asBits
}
