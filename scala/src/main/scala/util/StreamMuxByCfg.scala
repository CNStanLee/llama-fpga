package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class StreamMuxByCfg[T <: Data](dataType: HardType[T], numOfPorts: Int) extends Component {

  case class Config() extends Bundle {
    val data = Bits(32 bits)

    def packLength = data.take(24).asUInt

    def select = data.takeHigh(8).asUInt
  }

  val io = new Bundle {
    val cfg = slave(Stream(Config()))
    val inputs = Vec(slave(Stream(dataType())), numOfPorts)
    val output = master(Stream(dataType()))
  }
  noIoPrefix()

  val deMux = new StreamMux(dataType(), numOfPorts)
  io.output << deMux.io.output.haltWhen(~io.cfg.valid)

  val cnt = UInt(24 bits).setAsReg().init(0)
  val cntOvf = cnt === io.cfg.packLength
  io.cfg.ready.clear()
  when(io.output.fire) {
    cnt := cnt + 1
    when(cntOvf) {
      cnt.clearAll()
      io.cfg.ready.set()
    }
  }

  deMux.io.select := io.cfg.select.resized
  for (i <- 0 until numOfPorts) {
    io.inputs(i) >> deMux.io.inputs(i)
  }
}
