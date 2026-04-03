package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object Fp16ScaleDown {
  def apply(src: Bits, rightShift: Int) = {
    val expo = src.drop(10).take(5).asUInt
    val newExpo = Mux(expo > rightShift, expo - rightShift, U(0)).asBits
    src.msb ## newExpo ## src.take(10)
  }

  def apply(src:Bits, rightShift:UInt)={
    val expo = src.drop(10).take(5).asUInt
    val newExpo = Mux(expo > rightShift, expo - rightShift, U(0)).asBits
    src.msb ## newExpo ## src.take(10)
  }

  def vec(src: Bits, rightShift: Int) = {
    val split = src.subdivideIn(16 bits)
    split.map(Fp16ScaleDown(_, rightShift)).asBits
  }

  def vec(src: Bits, rightShift: UInt) = {
    val split = src.subdivideIn(16 bits)
    split.map(Fp16ScaleDown(_, rightShift)).asBits
  }
}
