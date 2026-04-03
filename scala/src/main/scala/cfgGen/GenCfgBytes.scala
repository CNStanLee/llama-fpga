package cfgGen

import spinal.core._

import scala.language.postfixOps

object GenCfgBytes {
  def constant(firstDim: Int, secondDim: Int, tag: Int, isAxpy: Boolean = false, resAdd: Boolean = false) = {
    /*
    val = secondDim - 1
    low = val & 255
    high = val >> 8
    cfg = np.array([low, high, firstDim - 1, isAxpy + (resAdd << 1) + (tag << 2)], dtype=np.uint8)
     */
    val misc = (if (isAxpy) 1 else 0) + ((if (resAdd) 1 else 0) << 1) + (tag << 2)
    val ret = B(misc, 8 bits) ## B(firstDim - 1, 8 bits) ## B(secondDim - 1, 16 bits)
    ret
  }

  def variable(firstDim: Int, secondDim: UInt, tag: Int, isAxpy: Boolean = false, resAdd: Boolean = false) = {
    val misc = (if (isAxpy) 1 else 0) + ((if (resAdd) 1 else 0) << 1) + (tag << 2)
    val ret = B(misc, 8 bits) ## B(firstDim - 1, 8 bits) ## (secondDim - 1).resize(16 bits)
    ret
  }
}
