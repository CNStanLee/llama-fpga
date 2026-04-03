package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class StreamAxiFrameFifo[T <: Data](
                                     dataType: HardType[T],
                                     depth: Int,
                                     forFMax: Boolean,
                                     userBit: Int = -1,
                                     destBit: Int = -1,
                                     largeBank: Boolean = false,
                                     largeBankSplit: Int = -1
                                   ) extends Component {
  val io = new Bundle {
    val input = slave(Stream(AxiFrame(dataType(), userBit = userBit, destBit = destBit)))
    val output = master(Stream(AxiFrame(dataType(), userBit = userBit, destBit = destBit)))
  }

  if (largeBank) {
    val tDataFifo = new LargeBankFifo(dataType(), depth, forFMax = forFMax, split = largeBankSplit, shallow = true)
    tDataFifo.io.push.arbitrationFrom(io.input)
    tDataFifo.io.push.payload := io.input.tdata
    io.output.arbitrationFrom(tDataFifo.io.pop)
    io.output.tdata := tDataFifo.io.pop.payload
  }
  else {
    val tDataFifo = StreamFifo(dataType(), depth, forFMax = forFMax)
    tDataFifo.io.push.arbitrationFrom(io.input)
    tDataFifo.io.push.payload := io.input.tdata
    io.output.arbitrationFrom(tDataFifo.io.pop)
    io.output.tdata := tDataFifo.io.pop.payload
  }

  if (userBit > 0) {
    val tUserFifo = StreamFifo(Bits(userBit bits), depth)
    tUserFifo.io.push.valid := io.input.valid
    tUserFifo.io.push.payload := io.input.tuser
    tUserFifo.io.pop.ready := io.output.ready
    io.output.tuser := tUserFifo.io.pop.payload
  }

  if (destBit > 0) {
    val tDestFifo = StreamFifo(Bits(destBit bits), depth)
    tDestFifo.io.push.valid := io.input.valid
    tDestFifo.io.push.payload := io.input.tdest
    tDestFifo.io.pop.ready := io.output.ready
    io.output.tdest := tDestFifo.io.pop.payload
  }
}
