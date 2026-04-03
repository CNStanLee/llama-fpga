package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LUT2(init: BigInt) extends BlackBox {

  val generic = new Generic {
    val INIT = B(init, 4 bits)
  }

  val I0, I1 = in Bool()
  val O = out Bool()
}
