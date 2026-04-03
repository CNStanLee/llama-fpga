package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class LargeBankFifo[T <: Data](dataType: HardType[T], depth: Int, forFMax: Boolean, split: Int, shallow: Boolean = true) extends Component {

  val io = new Bundle {
    val push = slave(Stream(dataType))
    val pop = master(Stream(dataType))
    val occupancy = out UInt (log2Up(depth + 1) bits)
    val availability = out UInt (log2Up(depth + 1) bits)
  }

  val bitWidth = dataType.getBitsWidth
  val bitPerSlice = bitWidth / split

  val fifo = Array.fill(split)(new StreamFifo(Bits(bitPerSlice bit), depth, forFMax = forFMax))
  if (shallow) {
    fifo.foreach(_.logic.ram.addAttribute("ram_style", "distributed"))
    fifo.foreach(_.logic.ptr.push.addAttribute("max_fanout", 100))
    fifo.foreach(_.logic.ptr.pop.addAttribute("max_fanout", 100))
  }

  io.occupancy := fifo.head.io.occupancy
  io.availability := fifo.head.io.availability

  val inPydSplit = io.push.payload.asBits.subdivideIn(split slices)
  (fifo, inPydSplit).zipped.foreach(_.io.push.payload := _)

  val outPydMerge = Vec(fifo.map(_.io.pop.payload)).asBits
  io.pop.payload.assignFromBits(outPydMerge)

  fifo.foreach(_.io.push.valid := io.push.valid)
  io.push.ready := fifo.head.io.push.ready

  io.pop.valid := fifo.head.io.pop.valid
  fifo.foreach(_.io.pop.ready := io.pop.ready)

  //  fifo.head.io.push.arbitrationFrom(io.push)
  //  io.pop.arbitrationFrom(fifo.head.io.pop)
  //
  //  val pushVld = Vec(Bool(), split - 1)
  //  val popRdy = Vec(Bool(), split - 1)
  //  for (i <- 1 until split) {
  //    pushVld(i - 1) := io.push.fire
  //    popRdy(i - 1) := io.pop.fire
  //    pushVld(i - 1).addAttribute("keep", "true")
  //    popRdy(i - 1).addAttribute("keep", "true")
  //    pushVld(i - 1).addAttribute("dont_touch", "yes")
  //    popRdy(i - 1).addAttribute("dont_touch", "yes")
  //
  //    fifo(i).io.push.valid := pushVld(i - 1)
  //    fifo(i).io.pop.ready := popRdy(i - 1)
  //  }

}
