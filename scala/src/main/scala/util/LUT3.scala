package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LUT3(init: BigInt) extends BlackBox {

  val generic = new Generic {
    val INIT = B(init, 8 bits)
  }

  val I0, I1, I2 = in Bool()
  val O = out Bool()
}
