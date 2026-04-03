package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object GenAxiDataMoverCmd {

  def apply(addr: UInt, len: UInt, baseAddr: UInt, inc: Bool = True, eof: Bool = False, tag: Bits = B"0000", addrWidth: Int = 32) = {
    val ret = B"0000" ## tag ##
      (addr + baseAddr).resize(addrWidth) ##
      B"0" ##
      eof ##
      B"000000" ##
      inc ##
      len.resize(23)
    ret
  }

  def retStream(addr: UInt, len: UInt, baseAddr: UInt, inc: Bool = True, eof: Bool = False, tag: Bits = B"0000", addrWidth: Int = 32) = {
    val cmd = B"0000" ## tag ##
      (addr + baseAddr).resize(addrWidth) ##
      B"0" ##
      eof ##
      B"000000" ##
      inc ##
      len.resize(23)
    val ret = Stream(Fragment(Bits(cmd.getWidth bits)))
    ret.valid.set()
    ret.last.set()
    ret.fragment := cmd
    ret
  }

  def fromIndex(index: Flow[Fragment[UInt]], baseAddr: UInt, len: UInt, inc: Bool = True, tag: Bits = B"0000") = {
    val mulA = RegNext(index.payload)
    val mulB = RegNext(len)
    val addC = RegNext(baseAddr)
    val abc = mulA * mulB + addC
    val addr = RegNext(abc)

    val ret = Flow(Fragment(Bits(72 bits)))
    val lenDly = RegNext(mulB)
    val tagDly = RegNext(RegNext(tag))
    val last = Delay(index.last, 2, init = False)
    ret.valid := Delay(index.valid, 2, init = False)
    ret.last := last
    ret.fragment := B"0000" ## tagDly ##
      addr.resize(32) ##
      B"0" ##
      last ##
      B"000000" ##
      inc ##
      lenDly.resize(23)
    ret
  }

  def fromPackIndex(index: Flow[Fragment[UInt]], baseAddr: UInt, packCnt:UInt, lenPerIndex: UInt, tag: Bits = B"0000", dmaSplit: Int) = {
    require(isPow2(dmaSplit))
    val mulA = RegNext(index.payload)
    val mulB = RegNext(lenPerIndex.drop(log2Up(dmaSplit)).asUInt)
    val addC = RegNext(baseAddr)
    val abc = mulA * mulB + addC
    val addr = RegNext(abc)

    val ret = Flow(Fragment(Bits(72 bits)))
    val lenDly = RegNext(RegNext(packCnt) * RegNext(lenPerIndex))
    val tagDly = RegNext(RegNext(tag))
    val last = Delay(index.last, 2, init = False)
    ret.valid := Delay(index.valid, 2, init = False)
    ret.last := last
    ret.fragment := B"0000" ## tagDly ##
      addr.resize(32) ##
      B"0" ##
      last ##
      B"000000" ##
      True ##
      lenDly.resize(23)
    ret
  }

  def fromIndexWithPackMerge(index: Flow[Fragment[UInt]], baseAddr: UInt, lenPerIndex: UInt, tag: Bits = B"0000", dmaSplit: Int) = {
    val indexPair = ConsecutiveDetector(index)
    val baseAddrDly = RegNext(baseAddr)
    val lenDly = RegNext(lenPerIndex)
    val tagDly = RegNext(tag)
    val filterIndex = Flow(Fragment(UInt(index.fragment.getWidth bits)))
    filterIndex.valid := indexPair.valid
    filterIndex.last := indexPair.last
    filterIndex.fragment := indexPair.A
    fromPackIndex(filterIndex, baseAddrDly, indexPair.B, lenDly, tagDly, dmaSplit)
  }

//  def fromIndexSplitLen(index: Flow[Fragment[UInt]], baseAddr: UInt, len: UInt, inc: Bool = True, tag: Bits = B"0000", dmaSplit: Int) = {
//    require(isPow2(dmaSplit))
//    val mulA = RegNext(index.payload)
//    val mulB = RegNext(len.drop(log2Up(dmaSplit)).asUInt)
//    val addC = RegNext(baseAddr)
//    val abc = mulA * mulB + addC
//    val addr = RegNext(abc)
//
//    val ret = Flow(Fragment(Bits(72 bits)))
//    val lenDly = RegNext(RegNext(len))
//    val tagDly = RegNext(RegNext(tag))
//    val last = Delay(index.last, 2, init = False)
//    ret.valid := Delay(index.valid, 2, init = False)
//    ret.last := last
//    ret.fragment := B"0000" ## tagDly ##
//      addr.resize(32) ##
//      B"0" ##
//      last ##
//      B"000000" ##
//      inc ##
//      lenDly.resize(23)
//    ret
//  }
//
//  def fromMergeIndex(index: Flow[Fragment[UInt]], baseAddr: UInt, len: UInt, inc: Bool = True, tag: Bits = B"0000", dmaSplit: Int) = {
//    val indexPair = ConsecutiveDetector(index)
//    val baseAddrDly = Delay(baseAddr, 2)
//    val lenDly = RegNext(len)
//    val incDly = Delay(inc, 2)
//    val tagDly = Delay(tag, 2)
//    val filterIndex = Flow(Fragment(UInt(index.fragment.getWidth bits)))
//    filterIndex.valid := RegNext(indexPair.valid, init = False)
//    filterIndex.last := RegNext(indexPair.last, init = False)
//    filterIndex.fragment := RegNext(indexPair.A)
//    val packLen = RegNext(indexPair.B * lenDly)
//    fromIndexSplitLen(filterIndex, baseAddrDly, packLen, incDly, tagDly, dmaSplit)
//  }
}
