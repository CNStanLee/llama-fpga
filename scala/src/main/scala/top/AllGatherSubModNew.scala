package top

import adapter.{FlowGate, FlowMux}
import c2c.{AllGatherNode, AllReduce, LowLatencyNode}
import mlp.IndexReorder
import spinal.core._
import spinal.lib._
import util.{FlowAddLast, FlowThrowLast}

import scala.language.postfixOps

class AllGatherSubModNew(
                          id: Int,
                          numOfCore: Int,
                          width: Int,
                          mlpDim: Int,
                          resOut2NodeTag: Int,
                          p2sOut2NodeTag: List[Int],
                          index2NodeTag: Int,
                          dotOut2NodeTag: List[Int],
                          acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]]
                        ) extends Component {

  require(isPow2(numOfCore))

  val idWidth = log2Up(numOfCore)
  val partialMlpDim = mlpDim / numOfCore

  val io = new Bundle {
    val dotOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val resOut = slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
    val p2sOut = slave(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
    val allGatherOut = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val allReduceOut = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))

    val indexIn = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val gateIndexIn = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val ugIndexIn = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
    val indexOut = master(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
  }

  val c2c = if (numOfCore != 1) new Bundle {
    val from = slave(Stream(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth))))
    val to = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6, destBit = idWidth))))
  } else null

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.allReduceOut)
  util.AxiStreamSpecRenamer(io.allGatherOut)
  util.AxiStreamSpecRenamer(io.indexOut)

  val dotOut = FlowGate.keepTagWithFragment(FlowAddLast(io.dotOut, True), dotOut2NodeTag)
  val resOut = FlowGate.keepTagWithFragment(io.resOut, List(resOut2NodeTag))
  val p2sOut = FlowGate.keepTagWithFragment(io.p2sOut, p2sOut2NodeTag)
  //  val index = FlowGate.keepTagWithFragment(io.indexIn, List(index2NodeTag))
  val (nodeIn, _) = FlowMux(Vec(resOut, dotOut, p2sOut))

  val node = {
    if (numOfCore == 4) new LowLatencyNode(id, numOfCore, width)
    else new AllGatherNode(id, numOfCore, width)
  }
  node.io.input << nodeIn.m2sPipe

  if (numOfCore != 1) {
    if (numOfCore == 2) {
      node.io.to >> c2c.to
      node.io.from << c2c.from.toFlow.m2sPipe
    }
    else{
      node.io.to >> c2c.to
      node.io.from << c2c.from.toFlow.m2sPipe
    }
  }

  val reduce = new AllReduce(numOfCore, width, dotOut2NodeTag ++ List(resOut2NodeTag), acc_func)
  val nodeOutNoLast = FlowThrowLast(node.io.output)
  reduce.io.input << nodeOutNoLast
  reduce.io.output.m2sPipe() >> io.allReduceOut
  nodeOutNoLast.m2sPipe() >> io.allGatherOut

  //  val address = (node.io.output.tdata.asUInt + node.io.output.tdest.asUInt * partialMlpDim).resize(16).asBits
  //  val node2Index = Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6, destBit = idWidth)))
  //  node2Index.valid := node.io.output.valid & node.io.output.tuser === index2NodeTag
  //  node2Index.tuser := node.io.output.tuser
  //  node2Index.tdata := address
  //  node2Index.tdest := node.io.output.tdest
  //  node2Index.last := node.io.output.last

  val directIndex = Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6)))
  directIndex.valid := io.indexIn.valid & io.indexIn.tuser =/= index2NodeTag
  directIndex.tdata := io.indexIn.tdata
  directIndex.tuser := io.indexIn.tuser
  directIndex.last := io.indexIn.last

  //    val reorder = new IndexReorder(numOfCore, depth = 4096, tag = index2NodeTag)
  //  reorder.io.input << node2Index.m2sPipe()

  val (indexOut, _) = FlowMux(Vec(
    //    reorder.io.output.m2sPipe(),
    io.gateIndexIn.m2sPipe(),
    io.ugIndexIn.m2sPipe(),
    directIndex.m2sPipe()
  ))

  io.indexOut << indexOut
}
