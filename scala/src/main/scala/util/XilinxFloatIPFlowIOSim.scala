package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class XilinxFloatIPFlowIOSim(
                              val ipName: String,
                              val latency: Int,
                              val numOfOperand: Int,
                              val inputWidth: Int,
                              val outputWidth: Int,
                              val isAcc: Boolean
                            ) extends Component {

  val io = new Bundle {
    val a = if (!isAcc && numOfOperand >= 1) slave(Flow(Bits(inputWidth bits))) else null
    val b = if (!isAcc && numOfOperand >= 2) slave(Flow(Bits(inputWidth bits))) else null
    val c = if (!isAcc && numOfOperand >= 3) slave(Flow(Bits(inputWidth bits))) else null
    val r = if (!isAcc) master(Flow(Bits(outputWidth bits))) else null

    val accIn = if (isAcc) slave(Flow(Fragment(Bits(inputWidth bits)))) else null
    val accOut = if (isAcc) master(Flow(Fragment(Bits(outputWidth bits)))) else null
  }

  if (!isAcc && numOfOperand == 1) {
    io.r.valid := Delay(io.a.valid, latency, init = False)
    io.r.payload.clearAll()
  }
  if (!isAcc && numOfOperand == 2) {
    io.r.valid := Delay(io.a.valid && io.b.valid, latency, init = False)
    io.r.payload.clearAll()
  }
  if (!isAcc && numOfOperand == 3) {
    io.r.valid := Delay(io.a.valid && io.b.valid && io.c.valid, latency, init = False)
    io.r.payload.clearAll()
  }
  if (isAcc) {
    io.accOut.valid := Delay(io.accIn.valid, latency, init = False)
    io.accOut.last := Delay(io.accIn.last & io.accIn.valid, latency, init = False)
    io.accOut.fragment.clearAll()
  }

  if (!isAcc && numOfOperand >= 1) {
    io.a.setName("s_axis_a")
    AxiStreamSpecRenamer(io.a)
  }
  if (!isAcc && numOfOperand >= 2) {
    io.b.setName("s_axis_b")
    AxiStreamSpecRenamer(io.b)
  }
  if (!isAcc && numOfOperand >= 3) {
    io.c.setName("s_axis_c")
    AxiStreamSpecRenamer(io.c)
  }

  if (!isAcc) {
    io.r.setName("m_axis_result")
    AxiStreamSpecRenamer(io.r)
  }

  if (isAcc) {
    io.accIn.setName("s_axis_a")
    AxiStreamSpecRenamer(io.accIn)
    io.accOut.setName("m_axis_result")
    AxiStreamSpecRenamer(io.accOut)
  }

//  this.setDefinitionName(ipName)
}
