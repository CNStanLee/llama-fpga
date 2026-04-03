package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class StreamingRom[T <: Data](table: Seq[T]) extends Area {
  val rom = Mem(table.head, initialContent = table)
  rom.addAttribute("ram_style", "distributed")
  val popPre = Stream(Bool())
  val popPrePipe = popPre.m2sPipe()
  val popPreFire = popPre.fire
  popPre.valid.set()

  val addr = UInt(log2Up(rom.wordCount) bits).setAsReg().init(0)
  val addrOvf = addr === (rom.wordCount - 1)
  popPre.payload := addrOvf
  when(popPreFire) {
    addr := addr + 1
    when(addrOvf) {
      addr := 0
    }
  }
  val ret = Stream(Fragment(table.head))
  val rdOut = rom.readSync(addr, popPreFire)
  ret.arbitrationFrom(popPrePipe)
  ret.fragment := rdOut
  ret.last := popPrePipe.payload
}