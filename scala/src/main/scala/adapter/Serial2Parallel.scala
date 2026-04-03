package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Serial2Parallel(width: Int, bankLen: Int) extends Component {

  val serialWidth = width
  val parallelWidth = serialWidth * bankLen

  val io = new Bundle {
    val input = slave(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
    val output = master(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.output)

  val dataDly = RegNext(io.input.tdata)
  val userDly = RegNext(io.input.tuser)
  val vldDly = Bool().setAsReg().init(False)
  vldDly.addAttribute("max_fanout", 100)
  vldDly.addAttribute("keep", "true")
  vldDly := io.input.valid

  val outVld = Bool().setAsReg().init(False)
  outVld.addAttribute("max_fanout", 100)
  outVld.addAttribute("keep", "true")
  outVld := io.input.valid

  val outVldCond = Bool().setAsReg().init(False)
  outVldCond.addAttribute("max_fanout", 100)

  val cnt = UInt(log2Up(bankLen) bits).setAsReg().init(0)
  val cntAboutToOvf = cnt === bankLen - 2
  val cntOvf = cnt === bankLen - 1
  when(vldDly) {
    cnt := cnt + 1
    when(cntAboutToOvf) {
      outVldCond.set()
    }
    when(cntOvf) {
      outVldCond.clear()
      cnt.clearAll()
    }
  }

//  cnt.addAttribute("mark_debug", "true")

  io.output.tdata := History(dataDly, bankLen, when = vldDly).reverse.asBits
  io.output.valid := outVldCond & vldDly
  io.output.tuser := userDly
}
