package quant

import adapter.{FlowGate, FlowMux, TagMap}
import spinal.core._
import spinal.lib._
import util._

import scala.language.postfixOps

class QuantWrapperTest(
                    quantWidth: Int,
                    headDim: Int,
                    maxIntFP16Init: Int,
                    lt_func: (Flow[Bits], Flow[Bits]) => Flow[Bool],
                    sub_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    convert_func: Flow[Bits] => Flow[Bits]
                  ) extends Component {

  val io = new Bundle {
    val toBeQuant = slave(Flow(Bits(16 bits)))
    val quantZero = master(Flow(Bits(quantWidth bits)))
    val quantScale = master(Flow(Bits(16 bits)))
    val afterQuant = master(Flow(Fragment(Bits(quantWidth bits))))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.toBeQuant)
  util.AxiStreamSpecRenamer(io.quantZero)
  util.AxiStreamSpecRenamer(io.quantScale)
  util.AxiStreamSpecRenamer(io.afterQuant)

  val toBeQuant = io.toBeQuant.m2sPipe

  val find = new FindRange(lt_func)
  val getCfg = new GetScaleZero(quantWidth, maxIntFP16Init, sub_func, div_func, convert_func)
  val quant = new LinearQuant(quantWidth, div_func, convert_func)

  find.cfg.length := U(headDim - 1, 16 bits)

  find.io.x << toBeQuant.translateWith(toBeQuant.payload)
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

object QuantWrapperTest extends App{
  SpinalVerilog(new QuantWrapperTest(
    quantWidth = 8,
    headDim = 128,
    maxIntFP16Init = 0x5bf8,
    lt_func = util.fp16lt0.lt_async,
    sub_func = util.fp16sub8.sub,
    div_func = util.fp16div12.div,
    convert_func = util.fp16toint9d4.to
  ))
}