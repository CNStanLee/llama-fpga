package util

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axi._
import spinal.lib.bus.amba4.axis._

import scala.language.postfixOps

class HBMWrapper(
                  addressWidth: Int = 33,
                  dataWidth: Int = 256,
                  channel: Int = 32,
                  moduleName: String = "HBM"
                ) extends BlackBox {

  val HBM_REF_CLK_0 = in Bool()
  val HBM_REF_CLK_1 = in Bool()
  val DRAM_0_STAT_CATTRIP = out Bool()
  val DRAM_0_STAT_TEMP = out Bits (7 bits)
  val apb_complete_0 = out Bool()
  val DRAM_1_STAT_CATTRIP = out Bool()
  val DRAM_1_STAT_TEMP = out Bits (7 bits)
  val apb_complete_1 = out Bool()

  val io = new Bundle {
    val AXI = Vec(slave(HBMAXIBundle(addressWidth = addressWidth, dataWidth = dataWidth)), channel)
  }
  noIoPrefix()
  for (i <- 0 until channel) {
    val prefix = "AXI_" + (if (i < 10) "0" + i.toString else i.toString)
    io.AXI(i).setName(prefix)
  }
}

class HBMUser(
               addressWidth: Int = 33,
               dataWidth: Int = 256,
               channel: Int = 32,
               moduleName: String = "HBM"
             ) extends Component {

  val HBM_REF_CLK_0 = in Bool()
  val HBM_REF_CLK_1 = in Bool()
  val wrCmd = Vec(slave(Stream(util.Linked(UInt(addressWidth bits), UInt(4 bits)))), channel)
  val rdCmd = Vec(slave(Stream(util.Linked(UInt(addressWidth bits), UInt(4 bits)))), channel)
  val wData = Vec(slave(Stream(Fragment(Bits(dataWidth bits)))), channel)
  val rData = Vec(master(Stream(Fragment(Bits(dataWidth bits)))), channel)

  val hbm = new HBMWrapper(addressWidth = addressWidth, dataWidth = dataWidth, channel = channel)
  hbm.HBM_REF_CLK_0 := HBM_REF_CLK_0
  hbm.HBM_REF_CLK_1 := HBM_REF_CLK_1

  for (i <- 0 until channel) {

    hbm.io.AXI(i).AWVALID := wrCmd(i).valid
    hbm.io.AXI(i).AWADDR := wrCmd(i).payload.A.asBits
    hbm.io.AXI(i).AWID.clearAll()
    hbm.io.AXI(i).AWLEN := wrCmd(i).payload.B.asBits
    hbm.io.AXI(i).AWSIZE := B"101"
    hbm.io.AXI(i).AWBURST := B"01"
    wrCmd(i).ready := hbm.io.AXI(i).AWREADY

    hbm.io.AXI(i).ARVALID := rdCmd(i).valid
    hbm.io.AXI(i).ARADDR := rdCmd(i).payload.A.asBits
    hbm.io.AXI(i).ARID.clearAll()
    hbm.io.AXI(i).ARLEN := rdCmd(i).payload.B.asBits
    hbm.io.AXI(i).ARSIZE := B"101"
    hbm.io.AXI(i).ARBURST := B"01"
    rdCmd(i).ready := hbm.io.AXI(i).ARREADY

    hbm.io.AXI(i).WVALID := wData(i).valid
    hbm.io.AXI(i).WDATA := wData(i).payload
    hbm.io.AXI(i).WSTRB.setAll()
    hbm.io.AXI(i).WLAST := wData(i).last
    wData(i).ready := hbm.io.AXI(i).WREADY

    rData(i).valid := hbm.io.AXI(i).RVALID
    rData(i).payload := hbm.io.AXI(i).RDATA
    rData(i).last := hbm.io.AXI(i).RLAST
    hbm.io.AXI(i).RREADY := rData(i).ready

    hbm.io.AXI(i).BREADY.set()

    hbm.io.AXI(i).WDATA_PARITY.clearAll()

    hbm.io.AXI(i).ACLK := clockDomain.readClockWire
    hbm.io.AXI(i).ARESET_N := clockDomain.readResetWire
  }
}

class HBMInst() extends Component {
  val hbm = new HBMUser(addressWidth = 33, dataWidth = 256, channel = 2)
  val HBM_REF_CLK_0 = hbm.HBM_REF_CLK_0.toIo()
  val HBM_REF_CLK_1 = hbm.HBM_REF_CLK_1.toIo()
  for (i <- 0 until 2) {
    val wrCmd = hbm.wrCmd(i).toIo()
    val rdCmd = hbm.rdCmd(i).toIo()
    val wData = hbm.wData(i).toIo()
    val rData = hbm.rData(i).toIo()
  }
}

object HBMInst extends App {
  SpinalVerilog(new HBMInst())
}