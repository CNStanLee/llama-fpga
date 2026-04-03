package quant

import adapter.{FlowGate, FlowMux, TagMap}
import spinal.core._
import spinal.lib._
import util._

import scala.language.postfixOps

class QuantWrapper(
                    busWidth: Int,
                    quantWidth: Int,
                    headDim: Int,
                    maxIntFP16Init: Int,
                    numOfPort: Int,
                    tagMap: List[(Int, Int)],
                    lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
                    sub_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    convert_func: Flow[Bits] => Flow[Bits]
                  ) extends Component {

  val io = new Bundle {
    val toBeQuant = Vec(slave(Flow(util.AxiFrame(Bits(16 bits), userBit = 6))), numOfPort)
    val quantZero = master(Flow(Bits(quantWidth bits)))
    val quantScale = master(Flow(Bits(16 bits)))
    val afterQuant = master(Flow(Fragment(Bits(quantWidth bits))))
  }

  //  val (toBeQuantTgM, toBeQuantErr) = TagMap(io.toBeQuant, tagMap)
  //  val toBeQuant = toBeQuantTgM.m2sPipe()

  val kToBeQuant = TagMap.gate(io.toBeQuant(0), tagMap.take(2)).m2sPipe
  val vToBeQuant = TagMap.gate(io.toBeQuant(1), tagMap.drop(2)).m2sPipe
  val (toBeQuant, _) = FlowMux(Vec(kToBeQuant, vToBeQuant))

  util.AxiStreamSpecRenamer(toBeQuant)

  val find = new FindRange(lt_func)
  val getCfg = new GetScaleZero(quantWidth, maxIntFP16Init, sub_func, div_func, convert_func)
  val quant = new LinearQuant(quantWidth, div_func, convert_func)

  find.cfg.length := U(headDim - 1, 16 bits)

  find.io.x << toBeQuant.translateWith(toBeQuant.tdata)
  find.io.min >> getCfg.io.min
  find.io.max >> getCfg.io.max

  val xFifo = new StreamFifo(Bits(16 bits), 1024, forFMax = true)
  xFifo.io.push.valid := find.io.x.valid
  xFifo.io.push.payload := find.io.x.payload

  val scaleFifo = new StreamFifo(Bits(16 bits), 32, forFMax = true)
  val scaleRep = scaleFifo.io.pop.repeat(headDim)._1.m2sPipe()
  scaleFifo.io.push.valid := getCfg.io.scale.valid
  scaleFifo.io.push.payload := getCfg.io.scale.payload

  val zeroFifo = new StreamFifo(Bits(quantWidth bits), 32, forFMax = true)
  val zeroRep = zeroFifo.io.pop.repeat(headDim)._1.m2sPipe()
  zeroFifo.io.push.valid := getCfg.io.zero.valid
  zeroFifo.io.push.payload := getCfg.io.zero.payload

  val joinEvent = StreamJoin(xFifo.io.pop.toEvent(), scaleRep.toEvent())
  val joinEventFire = joinEvent.fire
  joinEvent.freeRun()

  quant.io.x.valid := joinEventFire
  quant.io.x.payload := xFifo.io.pop.payload
  quant.io.scale.valid := joinEventFire
  quant.io.scale.payload := scaleRep.payload
  quant.io.zero << zeroRep
  //  io.afterQuant << quant.io.q

  val afterQuant = Flow(Fragment(Bits(quantWidth bits)))

  val cnt = UInt(log2Up(headDim) bits).setAsReg().init(0)
  val cntOvf = cnt === headDim - 1
  when(afterQuant.valid) {
    cnt := cnt + 1
    when(cntOvf) {
      cnt := 0
    }
  }

  afterQuant.valid := quant.io.q.valid
  afterQuant.payload := quant.io.q.payload
  afterQuant.last := cntOvf & quant.io.q.valid

  io.afterQuant << afterQuant.m2sPipe
  io.quantScale << getCfg.io.scale.m2sPipe
  io.quantZero << getCfg.io.zero.m2sPipe
}
