package util

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axi._
import spinal.lib.bus.amba4.axis._

import scala.language.postfixOps

class AXIDataMoverWrapper(
                           memoryMapDataWidth: Int,
                           mm2sStreamDataWidth: Int,
                           s2mmStreamDataWidth: Int,
                           moduleName: String = "AXIDatamover",
                           cmdBytes: Int = 9
                         ) extends BlackBox {
  val io = new Bundle {
    val s_axis_s2mm = slave(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = s2mmStreamDataWidth / 8,
        useKeep = true,
        useLast = true
      )))
    val s_axis_s2mm_cmd = slave(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = cmdBytes,
        useKeep = false,
        useLast = false
      )))
    val s_axis_mm2s_cmd = slave(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = cmdBytes,
        useKeep = false,
        useLast = false
      )))
    val m_axi = master(Axi4(
      Axi4Config(
        addressWidth = cmdBytes * 8 - 40,
        dataWidth = memoryMapDataWidth,
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
      )))
    val m_axis_s2mm_sts = master(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = 1,
        useKeep = true,
        useLast = true
      )))
    val m_axis_mm2s_sts = master(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = 1,
        useKeep = true,
        useLast = true
      )))
    val m_axis_mm2s = master(Axi4Stream(
      Axi4StreamConfig(
        dataWidth = mm2sStreamDataWidth / 8,
        useKeep = true,
        useLast = true
      )))

    val m_axi_mm2s_aclk = in Bool()
    val m_axi_mm2s_aresetn = in Bool()

    val m_axis_mm2s_cmdsts_aclk = in Bool()
    val m_axis_mm2s_cmdsts_aresetn = in Bool()

    val m_axi_s2mm_aclk = in Bool()
    val m_axi_s2mm_aresetn = in Bool()

    val m_axis_s2mm_cmdsts_awclk = in Bool()
    val m_axis_s2mm_cmdsts_aresetn = in Bool()

    val mm2s_err = out Bool()
    val s2mm_err = out Bool()
  }

  noIoPrefix()

  setDefinitionName(moduleName)

  io.m_axi.aw.setName("m_axi_s2mm_aw")
  io.m_axi.w.setName("m_axi_s2mm_w")
  io.m_axi.b.setName("m_axi_s2mm_b")
  io.m_axi.ar.setName("m_axi_mm2s_ar")
  io.m_axi.r.setName("m_axi_mm2s_r")

  io.m_axi.r.id.removeStatement()
  io.m_axi.b.id.removeStatement()

  Axi4SpecRenamer(io.s_axis_s2mm)
  Axi4SpecRenamer(io.s_axis_s2mm_cmd)
  Axi4SpecRenamer(io.s_axis_mm2s_cmd)
  Axi4SpecRenamer(io.m_axi)
  Axi4SpecRenamer(io.m_axis_s2mm_sts)
  Axi4SpecRenamer(io.m_axis_mm2s_sts)
  Axi4SpecRenamer(io.m_axis_mm2s)

  //  mapClockDomain(clock = io.m_axi_mm2s_aclk, reset = io.m_axi_mm2s_aresetn, resetActiveLevel = LOW)
  //  mapClockDomain(clock = io.m_axis_mm2s_cmdsts_aclk, reset = io.m_axis_mm2s_cmdsts_aresetn, resetActiveLevel = LOW)
  //  mapClockDomain(clock = io.m_axi_s2mm_aclk, reset = io.m_axi_s2mm_aresetn, resetActiveLevel = LOW)
  //  mapClockDomain(clock = io.m_axis_s2mm_cmdsts_awclk, reset = io.m_axis_s2mm_cmdsts_aresetn, resetActiveLevel = LOW)
  mapClockDomain(clock = io.m_axi_mm2s_aclk)
  mapClockDomain(clock = io.m_axis_mm2s_cmdsts_aclk)
  mapClockDomain(clock = io.m_axi_s2mm_aclk)
  mapClockDomain(clock = io.m_axis_s2mm_cmdsts_awclk)
}
