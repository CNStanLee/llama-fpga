package rope

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class InvFreqRom(dim: Int, points: Int) extends Component {

  val io = new Bundle {
    val invFreq = master(Stream(Bits(32 bits)))
  }

  val expo = Range(0, dim, 2).map(x => x.toDouble / dim.toDouble)
  val base = 10000.0
  val scale = 2 * scala.math.Pi
  val invFreq = expo.map(x => points.toDouble / scale * 1.0 / scala.math.pow(base, x))
  val bits = invFreq.map(x => java.lang.Float.floatToRawIntBits(x.toFloat).toBigInt & 0xffffffff)

  val rom = Mem(Bits(32 bits), initialContent = bits.map(x => B(x, 32 bits)))

  val popPre = Event
  val addr = UInt(log2Up(rom.wordCount) bits).setAsReg().init(0)
  when(popPre.fire) {
    addr := addr + 1
  }
  val rdOut = rom.readSync(addr, popPre.ready)
  popPre.valid.set()

  io.invFreq.payload := rdOut
  io.invFreq.arbitrationFrom(popPre.m2sPipe())
}
