package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object TagMap {
  def apply(tag: Bits, map: List[(Int, Int)]): Bits = {
    val hit = map.map(kv => tag === kv._1).asBits()
    val vec = map.map(kv => B(kv._2, tag.getWidth bits))
    MuxOH(hit, vec)
  }

  def apply[T <: Data](input: Flow[util.AxiFrame[T]], tagMap: List[(Int, Int)]): Flow[util.AxiFrame[T]] = {
    require(input.userBit > 0)
    val tag = input.tuser
    val newTag = apply(tag, tagMap)
    val output = Flow(util.AxiFrame(input.tdata, userBit = input.userBit, destBit = input.destBit))
    output.valid := input.valid
    output.tdata := input.tdata
    output.tuser := newTag
    if (input.destBit > 0)
      output.tdest := input.tdest
    output
  }

  def apply[T <: Data](input: Vec[Flow[util.AxiFrame[T]]], tagMap: List[(Int, Int)]): (Flow[util.AxiFrame[T]],Bool) = {
    val tag = tagMap.map(_._1)
    val gated = input.map(i => FlowGate.keepTag(i, tag))
    val (muxOut,err) = FlowMux(Vec(gated))
    val mapOut = apply(muxOut, tagMap)
    (mapOut,err)
  }

  def gate[T <: Data](input: Flow[util.AxiFrame[T]], tagMap: List[(Int, Int)]): Flow[util.AxiFrame[T]] = {
    require(input.userBit > 0)
    val tag = input.tuser
    val hit = tagMap.map(_._1 === input.tuser).orR
    val newTag = apply(tag, tagMap)
    val output = Flow(util.AxiFrame(input.tdata, userBit = input.userBit, destBit = input.destBit))
    output.valid := input.valid & hit
    output.tdata := input.tdata
    output.tuser := newTag
    if (input.destBit > 0)
      output.tdest := input.tdest
    output
  }
}
