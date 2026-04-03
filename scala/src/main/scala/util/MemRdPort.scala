package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

case class MemRdPort[T <: Data](dataType: HardType[T], depth: Int) extends Bundle with IMasterSlave {
  val cmd = Flow(UInt(log2Up(depth) bit))
  val rsp = dataType()

  override def asMaster(): Unit = {
    master(cmd)
    in(rsp)
  }
}
