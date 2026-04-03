package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

case class HBMAXIBundle(addressWidth: Int = 32, dataWidth: Int) extends Bundle with IMasterSlave {

  val strbWidth = dataWidth / 8
  val burstWidth = 2
  val sizeWidth = 3
  val lenWidth = 4
  val idWidth = 6
  val respWidth = 2

  val ACLK = Bool()
  val ARESET_N = Bool()

  val AWVALID = Bool()
  val AWREADY = Bool()
  val AWADDR = Bits(addressWidth bits)
  val AWID = Bits(idWidth bits)
  val AWLEN = Bits(lenWidth bits)
  val AWSIZE = Bits(sizeWidth bits)
  val AWBURST = Bits(burstWidth bits)

  val ARVALID = Bool()
  val ARREADY = Bool()
  val ARADDR = Bits(addressWidth bits)
  val ARID = Bits(idWidth bits)
  val ARLEN = Bits(lenWidth bits)
  val ARSIZE = Bits(sizeWidth bits)
  val ARBURST = Bits(burstWidth bits)

  val WVALID = Bool()
  val WREADY = Bool()
  val WDATA = Bits(dataWidth bits)
  val WSTRB = Bits(strbWidth bits)
  val WLAST = Bool()

  val RVALID = Bool()
  val RREADY = Bool()
  val RDATA = Bits(dataWidth bits)
  val RLAST = Bool()
  val RID = Bits(idWidth bits)
  val RRESP = Bits(respWidth bits)

  val BVALID = Bool()
  val BREADY = Bool()
  val BID = Bits(idWidth bits)
  val BRESP = Bits(respWidth bits)

  val WDATA_PARITY = Bits(32 bits)
  val RDATA_PARITY = Bits(32 bits)

  override def asMaster(): Unit = {
    out(ACLK, ARESET_N)

    out(AWVALID, AWADDR, AWID, AWLEN, AWSIZE, AWBURST)
    in(AWREADY)

    out(ARVALID, ARADDR, ARID, ARLEN, ARSIZE, ARBURST)
    in(ARREADY)

    out(WVALID, WDATA, WSTRB, WLAST)
    in(WREADY)

    in(RVALID, RDATA, RLAST, RID, RRESP)
    out(RREADY)

    in(BVALID, BID, BRESP)
    out(BREADY)

    in(RDATA_PARITY)
    out(WDATA_PARITY)
  }
}
