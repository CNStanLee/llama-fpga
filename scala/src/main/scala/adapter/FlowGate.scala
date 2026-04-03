package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object FlowGate {

  def apply[T <: Data](input: Flow[util.AxiFrame[T]], tag: List[Int]) = {
    val cond = tag.map(t => input.tuser === B(t)).reduce(_ || _)
    val ret = Flow(input.dataType)
    ret.valid := input.valid && cond
    ret.payload := input.tdata
    ret
  }

  def fragment[T <: Data](input: Flow[Fragment[util.AxiFrame[T]]], tag: List[Int]) = {
    val cond = tag.map(t => input.tuser === B(t)).reduce(_ || _)
    val ret = Flow(Fragment(input.dataType))
    ret.valid := input.valid && cond
    ret.fragment := input.tdata
    ret.last := input.last
    ret
  }

  def keepTag[T <: Data](input: Flow[util.AxiFrame[T]], tag: List[Int]) = {
    val cond = tag.map(t => input.tuser === B(t)).reduce(_ || _)
    val ret = Flow(util.AxiFrame(input.dataType, userBit = input.userBit, destBit = input.destBit))
    ret.valid := input.valid && cond
    ret.tdata := input.tdata
    if (input.userBit > 0) ret.tuser := input.tuser
    if (input.destBit > 0) ret.tdest := input.tdest
    ret
  }

  def keepTagWithFragment[T <: Data](input: Flow[Fragment[util.AxiFrame[T]]], tag: List[Int]) = {
    val cond = tag.map(t => input.tuser === B(t)).reduce(_ || _)
    val ret = Flow(Fragment(util.AxiFrame(input.dataType, userBit = input.userBit, destBit = input.destBit)))
    ret.valid := input.valid && cond
    ret.tdata := input.tdata
    ret.last := input.last
    if (input.userBit > 0) ret.tuser := input.tuser
    if (input.destBit > 0) ret.tdest := input.tdest
    ret
  }
}
