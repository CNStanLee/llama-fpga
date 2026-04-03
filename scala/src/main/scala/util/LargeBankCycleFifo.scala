package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LargeBankCycleFifo[T <: Data](dataType: HardType[T], depth: Int, split: Int, reuseWidth: Int = 8, shallow: Boolean = true) extends Component {

  val io = new Bundle {
    val push = slave(Stream(dataType))
    val pop = master(Stream(dataType))
    val reuse = in UInt (reuseWidth bits)
    val length = in UInt (log2Up(depth) bits)
  }

  val bitWidth = dataType.getBitsWidth
  val bitPerSlice = bitWidth / split

  val fifo = Array.fill(split)(new StreamCycleFifo(Bits(bitPerSlice bit), depth, reuseWidth))

  fifo.foreach(_.io.reuse := io.reuse)
  fifo.foreach(_.io.length := io.length)

  if (shallow) {
    fifo.foreach(_.ram.addAttribute("ram_style", "distributed"))
  }

  val inPydSplit = io.push.payload.asBits.subdivideIn(split slices)

  fifo.foreach(_.io.push.valid := io.push.valid)
  io.push.ready := fifo.head.io.push.ready
  (fifo, inPydSplit).zipped.foreach(_.io.push.payload := _)

  val outPydMerge = Vec(fifo.map(_.io.pop.payload)).asBits
  io.pop.valid := fifo.head.io.pop.valid
  fifo.foreach(_.io.pop.ready := io.pop.ready)
  io.pop.payload.assignFromBits(outPydMerge)
}
