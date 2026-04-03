package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class URAM16x16384Fifo() extends Component {

  val io = new Bundle {
    val push = slave(Stream(Fragment(Bits(16 bits))))
    val pop = master(Stream(Fragment(Bits(16 bits))))
  }

  val fifo = new StreamFifoPipe(Bits(72 bits), 4096, forFMax = true)
  fifo.logic.ram.addAttribute("ram_style", "ultra")

  val fifoPop = fifo.io.pop

  val adaptIn = Stream(Fragment(Bits(64 bits)))
  val inAdapter = StreamFragmentWidthAdapter(io.push, adaptIn, earlyLast = true)
  val vldElemIn = (CountOne(inAdapter.dataMask) - 1).resize(2)

  fifo.io.push.arbitrationFrom(adaptIn)
  fifo.io.push.payload := adaptIn.last ## B"00000" ## vldElemIn ## adaptIn.fragment

  val vldElemOut = fifoPop.payload.drop(64).take(2).asUInt
  val lastOut = fifoPop.payload.msb

  val pop = Stream(Fragment(Bits(16 bits)))
  val cnt = UInt(2 bits).setAsReg().init(0)
  val cntOvf = cnt === vldElemOut
  fifoPop.ready.clear()
  when(pop.fire){
    cnt := cnt + 1
    when(cntOvf){
      cnt.clearAll()
      fifoPop.ready.set()
    }
  }

  val selectSlice = fifoPop.payload.take(64).subdivideIn(4 slices)(cnt)
  pop.valid := fifoPop.valid
  pop.fragment := selectSlice
  pop.last := cntOvf & lastOut

  io.pop << pop.m2sPipe()
}

object URAM16x16384Fifo {
  def main(args: Array[String]): Unit = {
    SpinalVerilog(new URAM16x16384Fifo())
  }
}