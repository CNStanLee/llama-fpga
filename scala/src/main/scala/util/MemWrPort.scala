package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

case class MemWrPort[T <: Data](dataType: HardType[T], depth: Int) extends Bundle {
  val address = UInt(log2Up(depth) bits)
  val data = dataType()
}
