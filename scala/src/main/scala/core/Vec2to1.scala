package core

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Vec2to1(
               width: Int,
               length: Int,
               delay: Int,
               func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
             ) extends Component {

  val io = new Bundle {
    val in0 = slave(Flow(Vec(Bits(width bits), length)))
    val in1 = slave(Flow(Vec(Bits(width bits), length)))
    val res = master(Flow(Vec(Bits(width bits), length)))
  }

  val flowVec0 = Vec(Flow(Bits(width bits)), length)
  flowVec0.foreach(_.valid.set())
  (flowVec0, io.in0.payload).zipped.foreach(_.payload := _)

  val flowVec1 = Vec(Flow(Bits(width bits)), length)
  flowVec1.foreach(_.valid.set())
  (flowVec1, io.in1.payload).zipped.foreach(_.payload := _)

  val res = (flowVec0, flowVec1).zipped.map(func(_, _))
  (io.res.payload, res).zipped.foreach(_ := _.payload)

  io.res.valid := Delay(io.in0.valid && io.in1.valid, delay, init = False)
}
