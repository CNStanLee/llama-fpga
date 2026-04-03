package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object Fp32ScaleDown {
  def apply(src: Bits, rightShift: Int) = {
    val expo = src.drop(23).take(8).asUInt
    val newExpo = Mux(expo > rightShift, expo - rightShift, U(0)).asBits
    src.msb ## newExpo ## src.take(23)
  }

  def apply(src: Bits, rightShift: UInt) = {
    val expo = src.drop(23).take(8).asUInt
    val newExpo = Mux(expo > rightShift, expo - rightShift, U(0)).asBits
    src.msb ## newExpo ## src.take(23)
  }
}
