package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object Fix48ToFp16 {

  val latency = util.fp32fix48d6.latency + util.fp32toFp16.latency

  def apply(in: Flow[Bits]): Flow[Bits] = {
    val fp32 = util.fp32fix48d6.from(in)
    val fp16 = util.fp32toFp16.to(fp32)
    fp16
  }

  def apply_sim(in: Flow[Bits]): Flow[Bits] = {
    val fp32 = util.fp32fix48d6.from_sim(in)
    val fp16 = util.fp32toFp16.to_sim(fp32)
    fp16
  }
}

object Fix32ToFp16 {

  val latency = util.fp32fix48d6.latency + util.fp32toFp16.latency

  def apply(in: Flow[Bits]): Flow[Bits] = {
    val fp32 = util.fp32fix48d6.from(in.translateWith(in.payload.asSInt.resize(48).asBits))
    val fp16 = util.fp32toFp16.to(fp32)
    fp16
  }

  def apply_sim(in: Flow[Bits]): Flow[Bits] = {
    val fp32 = util.fp32fix48d6.from_sim(in.translateWith(in.payload.asSInt.resize(48).asBits))
    val fp16 = util.fp32toFp16.to_sim(fp32)
    fp16
  }
}