package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object FlowMux {
  def apply[T <: Data](input: Vec[Flow[T]]) = {
    val output = Flow(input.head.payload)
    val oneHot = input.map(_.valid)
//    val err = CountOne(oneHot) > 1
    val err = False
    output.valid := oneHot.orR
    output.payload := MuxOH(
      input.map(_.valid),
      input.map(_.payload)
    )
    (output, err)
  }
}