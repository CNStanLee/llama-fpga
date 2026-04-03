package mlp

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class InsertCfg(insertTag: (Int, Int, Int, Int)) extends Component {

  val io = new Bundle {
    val cfgIn = slave(Stream(Bits(32 bits)))
    val cfgOut = master(Stream(Bits(32 bits)))
    val gtCnt = master(Flow(Bits(16 bits)))
    val index = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
  }

  val status = new Bundle {
    val enPredictor = in Bool()
  }

  val (predDTag, gTag, uTag, mlpOutTag) = insertTag

  val gtZeroCnt = Stream(util.AxiFrame(Bits(16 bits), userBit = 6))
  val cnt = new GtZeroCnt()
  cnt.io.index << io.index
  cnt.io.output >> gtZeroCnt

  val tag = io.cfgIn.payload.takeHigh(6)
  val hit = tag === predDTag || tag === uTag || tag === mlpOutTag || Mux(status.enPredictor, tag === gTag, False)

  val insertCfg = io.cfgIn.payload.drop(16) ## gtZeroCnt.tdata
  val cfg = Mux(hit, insertCfg, io.cfgIn.payload)

  gtZeroCnt.ready := hit & io.cfgIn.fire
  val cfgInHalt = io.cfgIn.haltWhen(hit & ~gtZeroCnt.valid).translateWith(cfg)
  io.cfgOut << cfgInHalt.s2mPipe().m2sPipe()

  io.gtCnt.valid := gtZeroCnt.fire
  io.gtCnt.payload := gtZeroCnt.tdata
}
