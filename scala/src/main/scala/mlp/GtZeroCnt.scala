package mlp

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class GtZeroCnt() extends Component {

  val io = new Bundle {
    val index = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val output = master(Stream(util.AxiFrame(Bits(16 bits), userBit = 6)))
  }

  val cnt = UInt(16 bits).setAsReg().init(0)
  when(io.index.valid) {
    cnt := cnt + 1
    when(io.index.last) {
      cnt.clearAll()
    }
  }

  val output = Stream(util.AxiFrame(Bits(16 bits), userBit = 6))
  output.valid := io.index.valid & io.index.last
  output.tdata := cnt.asBits
  output.tuser := io.index.tuser

  io.output << output.m2sPipe()
}
