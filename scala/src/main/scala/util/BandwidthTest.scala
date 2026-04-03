package util

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axi.{Axi4, Axi4Config, Axi4SpecRenamer}
import spinal.lib.bus.amba4.axilite.{AxiLite4, AxiLite4SlaveFactory, AxiLite4SpecRenamer}
import top.AxiLiteCtrl

import scala.language.postfixOps

class BandwidthTest() extends Component {

  val ctrl = slave(AxiLite4(32, 32))
  ctrl.setName("S00_AXIL")

  val m_axi_hp = Vec(master(Axi4(
    Axi4Config(
      addressWidth = 32,
      dataWidth = 128,
      idWidth = 4,
      useBurst = true,
      useCache = true,
      useId = true,
      useLen = true,
      useProt = true,
      useSize = true,
      arUserWidth = 4,
      awUserWidth = 4,
      useLast = true,
      useResp = true,
      useLock = false,
      useQos = false,
      useRegion = false
    ))), 4)

  for (i <- 0 until 4) {
    Axi4SpecRenamer(m_axi_hp(i))
  }
  AxiLite4SpecRenamer(ctrl)

  val dmaHp = new SplitAxiDatamover(512, 4, 1024, offsetTable = List.fill(16)(0), addressWidth = 32)
  (m_axi_hp, dmaHp.io.m_axi).zipped.foreach(_ << _)
  val aresetn = in Bool()

  dmaHp.aresetn := aresetn
  dmaHp.io.mm2s.freeRun()
  dmaHp.io.s2mm.valid.clear()
  dmaHp.io.s2mm.payload.clearAll()
  dmaHp.io.s2mmCmd.valid.clear()
  dmaHp.io.s2mmCmd.payload.clearAll()

  val ctrlTool = new AxiLite4SlaveFactory(ctrl)
  val baseAddr = UInt(32 bits).setAsReg().init(0)
  val enTransfer = Bool().setAsReg().init(False)
  val lenPerTransfer = UInt(23 bits).setAsReg().init(0)
  val pageSize = UInt(23 bits).setAsReg().init(0)
  val sliceCntBound = UInt(10 bits).setAsReg().init(0)
  ctrlTool.write(baseAddr, 0x00, 0)
  ctrlTool.write(enTransfer, 0x04, 0)
  ctrlTool.write(lenPerTransfer, 0x08, 0)
  ctrlTool.write(sliceCntBound, 0x0C, 0)
  ctrlTool.write(pageSize, 0x10, 0)

  //  val clockCycleAhead = UInt(16 bits).setAsReg().init(0)
  //  ctrlTool.write(clockCycleAhead, 0x18, 0)
  //  dmaHp.issueClkAhead := clockCycleAhead

  val sliceCnt = UInt(10 bits).setAsReg().init(0)
  val sliceBase = UInt(27 bits).setAsReg().init(0)
  val enInc = Bool()
  val cmdVld = Bool().setAsReg().init(True)
  val sliceCntOvf = sliceCnt === sliceCntBound
  when(enInc & cmdVld) {
    sliceCnt := sliceCnt + 1
    sliceBase := sliceBase + pageSize
    when(sliceCntOvf) {
      sliceCnt.clearAll()
      cmdVld.clear()
    }
  }

  val testCmd = GenAxiDataMoverCmd(addr = sliceBase, len = lenPerTransfer, baseAddr = baseAddr, inc = True, eof = sliceCntOvf)
  val testCmdStream = Stream(Bits(72 bits))
  testCmdStream.valid := cmdVld
  testCmdStream.payload := testCmd
  enInc := testCmdStream.fire

  val testCmdStreamPipe = testCmdStream.continueWhen(enTransfer).m2sPipe().m2sPipe()

  val cmdFifo = new StreamFifo(Bits(72 bits), 1024, forFMax = true)
  cmdFifo.io.push << testCmdStreamPipe
  dmaHp.io.mm2sCmd << cmdFifo.io.pop.m2sPipe()

  val fire = dmaHp.io.mm2s.fire
  val flag = Bool().setAsReg().init(False)
  flag.setWhen(fire)
  flag.clearWhen(fire & dmaHp.io.mm2s.last)

  fire.addAttribute("mark_debug", "true")

  val fireCnt = UInt(32 bits).setAsReg().init(0)
  when(fire) {
    fireCnt := fireCnt + 1
  }

  val cnt = UInt(24 bits).setAsReg().init(0)
  when(flag) {
    cnt := cnt + 1
  }

  ctrlTool.read(cnt, 0x14, 0)
  ctrlTool.read(fireCnt, 0x1C, 0)

  m_axi_hp.foreach(_.r.valid.addAttribute("mark_debug", "true"))
  m_axi_hp.foreach(_.r.ready.addAttribute("mark_debug", "true"))

  flag.addAttribute("mark_debug", "true")
  fireCnt.addAttribute("mark_debug", "true")
  cnt.addAttribute("mark_debug", "true")
}

object BandwidthTest extends App {
  val clock = new ClockDomainConfig(resetActiveLevel = LOW)
  val cfg = SpinalConfig(
    defaultConfigForClockDomains = clock,
    inlineRom = true,
    nameWhenByFile = false,
    anonymSignalPrefix = "t"
  )
  cfg.generateVerilog(new BandwidthTest())
}