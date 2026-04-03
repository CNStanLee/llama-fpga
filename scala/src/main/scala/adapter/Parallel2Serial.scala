package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Parallel2Serial(width: Int, bankLen: Int) extends Component {

  val serialWidth = width
  val parallelWidth = serialWidth * bankLen

  val io = new Bundle {
    val input = slave(Stream(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val output = master(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
  }

  val input = io.input.translateWith(io.input.tdata)
  val output = Stream(Bits(serialWidth bits))
  StreamWidthAdapter(input, output)
  output.ready.set()
  io.output.valid := output.valid
  io.output.tdata := output.payload
  io.output.tuser := io.input.tuser
}
