package c2c

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class Node(id: Int, numOfCore: Int, width: Int) extends Component {

  require(isPow2(numOfCore))

  val idWidth = log2Up(numOfCore)

  val io = new Bundle {
    val input = slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
    val output = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth))))
    val from = if (numOfCore != 1) slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth)))) else null
    val to = if (numOfCore != 1) master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth)))) else null
  }

  val singleCore = (numOfCore == 1) generate new Area {
    io.output.valid := RegNext(io.input.valid, init = False)
    io.output.tdata := RegNext(io.input.payload.tdata)
    io.output.tuser := RegNext(io.input.payload.tuser, init = B(0))
    io.output.last := RegNext(io.input.payload.last, init = False)
    io.output.tdest := 0
  }
}
