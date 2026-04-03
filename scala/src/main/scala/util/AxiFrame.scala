package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

case class AxiFrame[T <: Data](dataType: HardType[T], userBit: Int = -1, destBit: Int = -1) extends Bundle {
  val tdata = dataType()
  val tuser = if (userBit >= 0) Bits(userBit bits) else null
  val tdest = if (destBit >= 0) Bits(destBit bits) else null

  this.setPartialName("")
}
