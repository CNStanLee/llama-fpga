package util

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axis._
import util.{GenAxiDataMoverCmd, StreamFifoPipe}

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class GenSplitAlignTransfer(addr: BigInt, len: BigInt, align: Int) extends Component {

  val base = in UInt (32 bits)

  val splitCnt = scala.math.ceil(len.toFloat / align.toFloat).toInt
  val enCnt = Bool()
  val cnt = UInt(log2Up(splitCnt) bits).setAsReg().init(0)
  val cntNext = UInt(log2Up(splitCnt) bits)
  val cntOvf = Bool().setAsReg().init(False)
  val cntOvfNext = cntNext === splitCnt - 1
  cnt := cntNext
  cntNext := cnt
  cntOvf := cntOvfNext

  val offset = UInt(32 bits).setAsReg().init(addr)
  val offsetNext = UInt(32 bits)
  offset := offsetNext
  offsetNext := offset

  val cmdLen = UInt(23 bits).setAsReg().init(align)
  val cmdLenNext = Mux(cntOvfNext, U(len - (splitCnt - 1) * align), U(align))
  cmdLen := cmdLenNext.resized

  when(enCnt) {
    cntNext := cnt + 1
    offsetNext := offset + align
    when(cntOvf) {
      cntNext := 0
      offsetNext := addr
    }
  }

  val cmd = GenAxiDataMoverCmd(offset, cmdLen, base, inc = True, eof = cntOvf)
  val ret = master(Stream(Fragment(Bits(72 bits))))
  ret.valid.set()
  ret.fragment := cmd
  ret.last := cntOvf
  enCnt := ret.fire
}

object GenSplitAlignTransfer {
  def apply(addr: BigInt, len: BigInt, base: UInt, align: Int) = {
    val splitCnt = scala.math.ceil(len.toFloat / align.toFloat).toInt
    val enCnt = Bool()
    val cnt = UInt(log2Up(splitCnt) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(splitCnt) bits)
    val cntOvf = Bool().setAsReg().init(False)
    val cntOvfNext = cntNext === splitCnt - 1
    cnt := cntNext
    cntNext := cnt
    cntOvf := cntOvfNext

    val offset = UInt(32 bits).setAsReg().init(addr)
    val offsetNext = UInt(32 bits)
    offset := offsetNext
    offsetNext := offset

    val cmdLen = UInt(23 bits).setAsReg().init(align)
    val cmdLenNext = Mux(cntOvfNext, U(len - (splitCnt - 1) * align), U(align))
    cmdLen := cmdLenNext.resized

    when(enCnt) {
      cntNext := cnt + 1
      offsetNext := offset + align
      when(cntOvf) {
        cntNext := 0
        offsetNext := addr
      }
    }

    val cmd = GenAxiDataMoverCmd(offset, cmdLen, base, inc = True, eof = cntOvf)
    val ret = Stream(Fragment(Bits(72 bits)))
    ret.valid.set()
    ret.fragment := cmd
    ret.last := cntOvf
    enCnt := ret.fire
    ret
  }
}
