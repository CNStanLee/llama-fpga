package rope

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class CosSinGen(
                 points: Int
               ) extends Component {

  val pointWidth = log2Up(points)
  val byteAlignWidth = (pointWidth + 7) / 8 * 8

  val io = new Bundle {
    val index = slave(Stream(Bits(byteAlignWidth bits)))
    val sinCos = master(Stream(Bits(32 bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.index)
  util.AxiStreamSpecRenamer(io.sinCos)

  val scale = scala.math.Pi / 2.0
  val halfPoints = points / 2
  val quarterPoints = points / 4
  val quarterPointsMinusOne = quarterPoints - 1

  def readIntListFromFile(filename: String): List[Int] = {
    val source = scala.io.Source.fromFile(filename)
    val lines = try source.mkString finally source.close()
    lines.split(" ").map(_.toInt).toList
  }

  val table = readIntListFromFile("data/sin4096quarter.txt")
  val rom = Mem(Bits(16 bits), initialContent = table.map(x => B(x, 16 bits)))

  val popPre = io.index.toEvent()
  val index = io.index.payload.resize(pointWidth)

  val lowHalf = index.take(log2Up(halfPoints))
  val highHalf = index.drop(log2Up(halfPoints))
  val lowQuarter = lowHalf.take(log2Up(quarterPoints))
  val highQuarter = lowHalf.drop(log2Up(quarterPoints))

  val sinFlip = highQuarter.orR
  val sinNeg = highHalf.orR
  val sinNegDly = RegNextWhen(sinNeg, popPre.ready)
  val sinAddr = Mux(sinFlip, quarterPointsMinusOne - lowQuarter.asUInt, lowQuarter.asUInt)
  val sinRdOut = rom.readSync(sinAddr, popPre.ready)
  val negSinRdOut = B"1" ## sinRdOut.dropHigh(1)
  val sinVal = Mux(sinNegDly, negSinRdOut, sinRdOut)

  val cosFlip = (~highHalf.orR & ~highQuarter.orR) || (highHalf.orR & ~highQuarter.orR)
  val cosNeg = (~highHalf.orR & highQuarter.orR) || (highHalf.orR & ~highQuarter.orR)
  val cosNegDly = RegNextWhen(cosNeg, popPre.ready)
  val cosAddr = Mux(cosFlip, quarterPointsMinusOne - lowQuarter.asUInt, lowQuarter.asUInt)
  val cosRdOut = rom.readSync(cosAddr, popPre.fire)
  val negCosRdOut = B"1" ## cosRdOut.dropHigh(1)
  val cosVal = Mux(cosNegDly, negCosRdOut, cosRdOut)

  io.sinCos.payload := sinVal ## cosVal
  io.sinCos.arbitrationFrom(popPre.m2sPipe())
}

object CosSinGen extends App {
  val cfg = SpinalConfig(inlineRom = true)
  cfg.generateVerilog(new CosSinGen(1 << 14))
}