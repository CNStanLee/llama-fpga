package util

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axilite._
import spinal.lib.bus.amba4.axis._
import spinal.lib.bus.amba4.axi._

import scala.language.postfixOps

class AuroraInterfaceTest() extends Component {

  val config = Axi4StreamConfig(
    dataWidth = 8,
    useKeep = true,
    useLast = true
  )

  val io = new Bundle {
    val cfg = slave(AxiLite4(32, 32))
    val m_axis = master(Axi4Stream(config))
    val s_axis = slave(Axi4Stream(config))
  }

  noIoPrefix()
  AxiLite4SpecRenamer(io.cfg)
  Axi4SpecRenamer(io.m_axis)
  Axi4SpecRenamer(io.s_axis)
  io.s_axis.ready.set()

  val ctrl = new AxiLite4SlaveFactory(io.cfg)
  val length = UInt(32 bits).setAsReg().init(0)
  val start = Bool().setAsReg().init(False)
  ctrl.write(address = 0x00, bitMapping = (0, length))
  ctrl.read(address = 0x00, bitMapping = (0, length))
  ctrl.write(address = 0x04, bitMapping = (0, start))
  start.clear()

  val flag = Bool().setAsReg().init(False)
  flag.setWhen(start)
  val cnt = UInt(32 bits).setAsReg().init(0)
  when(io.m_axis.fire) {
    cnt := cnt + 1
    when(cnt === length) {
      cnt.clearAll()
      flag.clear()
    }
  }

  io.m_axis.valid := flag
  io.m_axis.data := cnt.resize(64).asBits
  io.m_axis.keep.setAll()
  io.m_axis.last := cnt === length
}

object AuroraInterfaceTest extends App {
  val config = SpinalConfig(defaultConfigForClockDomains = ClockDomainConfig(resetActiveLevel = LOW))
  config.generateVerilog(new AuroraInterfaceTest)
}
