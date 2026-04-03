package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class BottleNeckFifo(width: Int, depth: Int) extends Component {

  val io = new Bundle {
    val push = slave(Stream(Bits(width bits)))
    val pop = master(Stream(Bits(width bits)))
  }

  val totalBits = width * depth
  val uramWidth = 64
  val uramDepth = totalBits / uramWidth
  val fifo = new StreamFifo(Bits(uramWidth bits), uramDepth, forFMax = true)
  fifo.logic.ram.addAttribute("ram_style", "ultra")

  val pushIn = Stream(Bits(width bits))
  pushIn.valid := io.push.valid
  pushIn.payload := io.push.payload
  val pushInPipe = pushIn.m2sPipe()

  val adaptIn = Stream(Bits(uramWidth bits))
  StreamWidthAdapter(pushInPipe, adaptIn)

  val adaptOut = Stream(Bits(width bits))
  StreamWidthAdapter(fifo.io.pop, adaptOut)

  fifo.io.push << adaptIn.m2sPipe()
  io.pop << adaptOut
}
