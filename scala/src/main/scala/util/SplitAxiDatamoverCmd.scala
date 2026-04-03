package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class SplitAxiDatamoverCmd(split: Int, offsetTable: List[Int], addressWidth: Int = 32) extends Component {

  require(isPow2(split))

  val io = new Bundle {
    val inCmd = slave(Stream(Bits(addressWidth + 40 bits)))
    val outCmd = Vec(master(Stream(Bits(addressWidth + 40 bits))), split)
  }

  val addr = io.inCmd.payload.dropHigh(8).takeHigh(addressWidth).asUInt
  val len = io.inCmd.payload.takeLow(23)
  val inc = io.inCmd.payload(23)
  val eof = io.inCmd.payload(30)
  val tag = io.inCmd.payload.takeHigh(8).take(4).asUInt

  val noOffset = offsetTable.map(_ == 0).reduce(_ & _)

  val splitLen = len.dropLow(log2Up(split)).asUInt
  val offsetVec = if(!noOffset) Vec(offsetTable.map(U(_))) else null
  val offsetSel = if(!noOffset) offsetVec(tag) else U(0)
  val offsetBase = Mux(tag === 0, splitLen, offsetSel)
  val offset = (0 until split).map(i => offsetBase * i)

  val forks = new StreamFork(NoData(), split, synchronous = true)
  forks.io.input.arbitrationFrom(io.inCmd)

  val forkCmd = Vec(Stream(Bits(addressWidth + 40 bits)), split)
  for (i <- 0 until split) {
    forkCmd(i).arbitrationFrom(forks.io.outputs(i))
    forkCmd(i).payload := GenAxiDataMoverCmd(
      offset(i),
      splitLen,
      addr,
      inc = inc, eof = eof,
      tag = tag.asBits,
      addrWidth = addressWidth
    )
    io.outCmd(i) << forkCmd(i).m2sPipe()
  }
}
