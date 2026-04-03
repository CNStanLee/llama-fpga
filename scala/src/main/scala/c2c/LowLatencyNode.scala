package c2c

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class LowLatencyNode(id: Int, numOfCore: Int, width: Int, inFifoDepth: Int = 12288, cacheSize: Int = 32) extends Node(id, numOfCore, width) {

  val multiCore = (numOfCore > 1) generate new Area {

    val fifo = new StreamFifo(Fragment(util.AxiFrame(Bits(width bits), userBit = 6)), depth = inFifoDepth, forFMax = true)
    fifo.io.push << io.input.toStream

    val hit = Vec(Bool(), numOfCore)
    val hitNext = Vec(Bool(), numOfCore)
    val allHit = hit.reduce(_ & _)

    hit.foreach(_.setAsReg().init(False))
    (hit, hitNext).zipped.foreach(_ := _)
    (hitNext, hit).zipped.foreach(_ := _)

    val cnt = Vec(UInt(log2Up(cacheSize) bits), numOfCore)
    cnt.foreach(_.setAsReg().init(0))

    for (i <- 0 until numOfCore) {
      when(io.to.valid & io.to.tdest === i) {
        cnt(i) := cnt(i) + 1
        when(cnt(i) === cacheSize - 1) {
          hitNext(i).set()
          cnt(i).clearAll()
        }
      }
    }
    when(allHit) {
      hitNext.foreach(_.clear())
    }

    val inputCtrl = fifo.io.pop.haltWhen(hit(id))
    val insert = Bool()
    val muxVld = Mux(insert, inputCtrl.valid, io.from.valid)
    val muxLast = Mux(insert, inputCtrl.last, io.from.last)
    val muxData = Mux(insert, inputCtrl.tdata, io.from.tdata)
    val muxUser = Mux(insert, inputCtrl.tuser, io.from.tuser)
    val muxDest = Mux(insert, B(id, idWidth bits), io.from.tdest)

    insert := Mux(io.from.valid, io.from.tdest === id, True)
    inputCtrl.ready := insert

    io.to.valid := muxVld
    io.to.last := muxLast
    io.to.tdata := muxData
    io.to.tuser := muxUser
    io.to.tdest := muxDest

    val reorderCache = new Reorder(numOfCore, cacheSize * 2)
    reorderCache.io.input << io.to.m2sPipe
    io.output << reorderCache.io.output.m2sPipe

    //    io.input.valid.addAttribute("mark_debug", "true")
    //    io.from.valid.addAttribute("mark_debug", "true")
    //    io.from.tdest.addAttribute("mark_debug", "true")
    //    io.from.last.addAttribute("mark_debug", "true")
    //    io.to.valid.addAttribute("mark_debug", "true")
    //    io.to.tdest.addAttribute("mark_debug", "true")
    //    io.output.valid.addAttribute("mark_debug", "true")
  }
}
