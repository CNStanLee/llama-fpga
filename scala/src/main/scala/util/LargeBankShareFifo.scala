package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LargeBankShareFifo[T <: Data](dataType: HardType[T], depth: Int, split: Int, reuseWidth: Int = 8, shallow: Boolean = true) extends Component {

  val io = new Bundle {
    val push = slave(Stream(dataType))
    val pop = master(Stream(dataType))
    val reuse = in UInt (reuseWidth bits)
    val length = in UInt (log2Up(depth) bits)
    val selCycleOnPush = in Bool()
    val selCycleOnPop = in Bool()
  }

  val bitWidth = dataType.getBitsWidth
  val bitPerSlice = bitWidth / split

  val fifoCtrl = Array.fill(split)(new ShareFifoCtrl(Bits(bitPerSlice bit), depth / 2, reuseWidth))

  fifoCtrl.foreach(_.io.reuse := io.reuse)
  fifoCtrl.foreach(_.io.length := io.length.resized)
  fifoCtrl.foreach(_.io.selCycleOnPush := io.selCycleOnPush)
  fifoCtrl.foreach(_.io.selCycleOnPop := io.selCycleOnPop)

  val rams = Array.fill(split)(Mem(Bits(bitPerSlice bit), depth))

  if (shallow) {
    rams.foreach(_.addAttribute("ram_style", "distributed"))
  }

  for(i <-0 until split) {
    rams(i).write(
      enable = fifoCtrl(i).wrPort.valid,
      address = fifoCtrl(i).wrPort.address,
      data = fifoCtrl(i).wrPort.data
    )

    fifoCtrl(i).rdPort.rsp := rams(i).readSync(
      enable = fifoCtrl(i).rdPort.cmd.valid,
      address = fifoCtrl(i).rdPort.cmd.payload
    )
  }

  val inPydSplit = io.push.payload.asBits.subdivideIn(split slices)

  fifoCtrl.foreach(_.io.push.valid := io.push.valid)
  io.push.ready := fifoCtrl.head.io.push.ready
  (fifoCtrl, inPydSplit).zipped.foreach(_.io.push.payload := _)

  val outPydMerge = Vec(fifoCtrl.map(_.io.pop.payload)).asBits
  io.pop.valid := fifoCtrl.head.io.pop.valid
  fifoCtrl.foreach(_.io.pop.ready := io.pop.ready)
  io.pop.payload.assignFromBits(outPydMerge)
}

object LargeBankShareFifo extends App {
  SpinalVerilog(new LargeBankShareFifo(Bits(2048 bits), 64, 4, 8, true))
}