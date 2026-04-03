package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LargeBankFragmentFifo[T <: Data](dataType: HardType[T], depth: Int, forFMax: Boolean, split: Int, shallow: Boolean = true) extends Component {

  val io = new Bundle {
    val push = slave(Stream(Fragment(dataType)))
    val pop = master(Stream(Fragment(dataType)))
    val occupancy = out UInt (log2Up(depth + 1) bits)
    val availability = out UInt (log2Up(depth + 1) bits)
  }

  val bitWidth = dataType.getBitsWidth
  val bitPerSlice = bitWidth / split

  val fifo = Array.fill(split)(new StreamFifo(Bits(bitPerSlice bit), depth, forFMax = forFMax))
  val lastFifo = new StreamFifo(Bool(), depth, forFMax = forFMax)
  if (shallow) {
    fifo.foreach(_.logic.ram.addAttribute("ram_style", "distributed"))
    fifo.foreach(_.logic.ptr.push.addAttribute("max_fanout", 100))
    fifo.foreach(_.logic.ptr.pop.addAttribute("max_fanout", 100))
    lastFifo.logic.ram.addAttribute("ram_style", "distributed")
  }

  io.occupancy := fifo.head.io.occupancy
  io.availability := fifo.head.io.availability

  val inPydSplit = io.push.fragment.asBits.subdivideIn(split slices)
  (fifo, inPydSplit).zipped.foreach(_.io.push.payload := _)

  val outPydMerge = Vec(fifo.map(_.io.pop.payload)).asBits
  io.pop.fragment.assignFromBits(outPydMerge)

  fifo.foreach(_.io.push.valid := io.push.valid)
  io.push.ready := fifo.head.io.push.ready

  io.pop.valid := fifo.head.io.pop.valid
  fifo.foreach(_.io.pop.ready := io.pop.ready)

  lastFifo.io.push.valid := fifo.head.io.push.fire
  lastFifo.io.push.payload := io.push.last
  lastFifo.io.pop.ready := io.pop.ready
  io.pop.last := lastFifo.io.pop.payload
}
