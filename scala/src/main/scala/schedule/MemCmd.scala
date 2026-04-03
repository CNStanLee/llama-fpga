package schedule

import spinal.core._

import scala.language.postfixOps

case class MemCmd(addrWidth:Int, lenWidth:Int) extends Bundle {
  val addr = UInt(addrWidth bits)
  val len = UInt(lenWidth bits)
}
