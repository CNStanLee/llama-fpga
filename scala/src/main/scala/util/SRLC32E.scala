package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class SRLC32E() extends BlackBox {

  val generic = new Generic {
    val INIT = B(0,32 bits)
    val IS_CLK_INVERTED = B(0,1 bits)
  }

  val Q = out Bool()
  val Q31 = out Bool()
  val A = in Bits(5 bits)
  val CE = in Bool()
  val CLK = in Bool()
  val D = in Bool()

  mapClockDomain(clock = CLK)
}
