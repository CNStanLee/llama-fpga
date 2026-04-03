package util

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class XilinxFloatIPStreamIO(
                           val ipName: String,
                           val latency: Int,
                           val numOfOperand: Int,
                           val inputWidth: Int,
                           val outputWidth: Int,
                           val isAcc: Boolean
                         ) extends BlackBox {

  val io = new Bundle {
    val aclk = if (latency != 0) in Bool() else null
    val a = if (!isAcc && numOfOperand >= 1) slave(Stream(Bits(inputWidth bits))) else null
    val b = if (!isAcc && numOfOperand >= 2) slave(Stream(Bits(inputWidth bits))) else null
    val c = if (!isAcc && numOfOperand >= 3) slave(Stream(Bits(inputWidth bits))) else null
    val r = if (!isAcc) master(Stream(Bits(outputWidth bits))) else null

    val accIn = if (isAcc) slave(Stream(Fragment(Bits(inputWidth bits)))) else null
    val accOut = if (isAcc) master(Stream(Fragment(Bits(outputWidth bits)))) else null
  }
  noIoPrefix()

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

  if (latency != 0) mapClockDomain(clock = io.aclk)
  this.setDefinitionName(ipName)
}
