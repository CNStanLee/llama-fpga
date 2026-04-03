package util

import spinal.core._

import scala.language.postfixOps

object SimpleDualPortRam {
  def apply(wordWidth: Int, wordCount: Int, wrMaskWidth: Int = 1,
            wrMaskEnable: Boolean = false, rdLatency: Int = 1) = {
    new SimpleDualPortRam(
      wordWidth = wordWidth,
      wordCount = wordCount,
      wrAddressWidth = log2Up(wordCount),
      wrDataWidth = wordWidth,
      wrMaskWidth = wrMaskWidth,
      wrMaskEnable = wrMaskEnable,
      rdAddressWidth = log2Up(wordCount),
      rdDataWidth = wordWidth,
      rdLatency = rdLatency)
  }
}

class SimpleDualPortRam(
                         val wordWidth      : Int,
                         val wordCount      : Int,
                         val readUnderWrite : ReadUnderWritePolicy = dontCare,
                         val technology     : MemTechnologyKind = auto,

                         val wrClock        : ClockDomain = ClockDomain.current,
                         val wrAddressWidth : Int,
                         val wrDataWidth    : Int,
                         val wrMaskWidth    : Int = 1,
                         val wrMaskEnable   : Boolean = false,

                         val rdClock        : ClockDomain = ClockDomain.current,
                         val rdAddressWidth : Int,
                         val rdDataWidth    : Int,
                         val rdLatency      : Int = 1
                       ) extends BlackBox {

  addGenerics(
    "wordCount"      -> SimpleDualPortRam.this.wordCount,
    "wordWidth"      -> SimpleDualPortRam.this.wordWidth,
    "clockCrossing"  -> (wrClock != rdClock),
    "technology"     -> SimpleDualPortRam.this.technology.technologyKind,
    "readUnderWrite" -> SimpleDualPortRam.this.readUnderWrite.readUnderWriteString,
    "wrAddressWidth" -> SimpleDualPortRam.this.wrAddressWidth,
    "wrDataWidth"    -> SimpleDualPortRam.this.wrDataWidth,
    "wrMaskWidth"    -> SimpleDualPortRam.this.wrMaskWidth,
    "wrMaskEnable"   -> SimpleDualPortRam.this.wrMaskEnable,
    "rdAddressWidth" -> SimpleDualPortRam.this.rdAddressWidth,
    "rdDataWidth"    -> SimpleDualPortRam.this.rdDataWidth,
    "rdLatency"      -> SimpleDualPortRam.this.rdLatency
  )


  val io = new Bundle {
    val wr = new Bundle {
      val clk  = in Bool()
      val en   = in Bool()
      val mask = in Bits(wrMaskWidth bits)
      val addr = in UInt(wrAddressWidth bit)
      val data = in Bits(wrDataWidth bit)
    }

    val rd = new Bundle {
      val clk  = in Bool()
      val en   = in Bool()
      val addr = in UInt(rdAddressWidth bit)
      val dataEn = in Bool() default(True) //Only used if rdLatency > 1
      val data = out Bits(rdDataWidth bit)
    }
  }

  mapClockDomain(wrClock, io.wr.clk)
  mapClockDomain(rdClock, io.rd.clk)
  noIoPrefix()
}