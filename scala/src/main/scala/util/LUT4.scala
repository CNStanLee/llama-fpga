package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LUT4(init: BigInt) extends BlackBox {

  val generic = new Generic {
    val INIT = B(init, 16 bits)
  }

  val I0, I1, I2, I3 = in Bool()
  val O = out Bool()
}
