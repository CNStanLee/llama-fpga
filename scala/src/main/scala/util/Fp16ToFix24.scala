package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object Fp16ToFix24 {

  val latency = util.fp16toFp32.latency + util.fp32toFix24d6.latency

  def apply(in: Flow[Bits]): Flow[Bits] = {
    val fp16 = util.fp16toFp32.to(in)
    val fix24 = util.fp32toFix24d6.to(fp16)
    fix24
  }

  def apply_sim(in: Flow[Bits]): Flow[Bits] = {
    val fp16 = util.fp16toFp32.to_sim(in)
    val fix24 = util.fp32toFix24d6.to_sim(fp16)
    fix24
  }
}
