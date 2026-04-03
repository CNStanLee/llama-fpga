package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object Fp16ScaleUp {
  def apply(src: Bits, leftShift: Int) = {
    val expo = src.drop(10).take(5).asUInt
    val newExpo = (expo + leftShift).asBits
    src.msb ## newExpo ## src.take(10)
  }

  def apply(src: Bits, leftShift: UInt) = {
    val expo = src.drop(10).take(5).asUInt
    val newExpo = (expo + leftShift).asBits
    src.msb ## newExpo ## src.take(10)
  }

  def vec(src: Bits, leftShift: Int) = {
    val split = src.subdivideIn(16 bits)
    split.map(Fp16ScaleUp(_, leftShift)).asBits
  }
}
