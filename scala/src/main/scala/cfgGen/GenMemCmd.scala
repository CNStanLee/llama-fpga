package cfgGen

import adapter.FlowMux
import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axi._
import spinal.lib.bus.amba4.axis._
import util.{AXIDataMoverWrapper, GenAxiDataMoverCmd, StreamFifoPipe}

import java.awt.peer.TextAreaPeer
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class GenMemCmd(
                 numOfCore: Int,
                 busWidth: Int,
                 dim: Int,
                 mlpDim: Int,
                 predDim: Int,
                 head: Int,
                 layer: Int,
                 vocabSize: Int,
                 maxToken: Int,
                 baseAddr: Int,
                 extTokenTag: (Int, Int, Int),
                 dmaSplit: Int = 1
               ) extends Component {

  val bankLen = busWidth / 4

  val dataMoverAxisCfg = Axi4StreamConfig(
    dataWidth = busWidth / 8,
    useLast = true,
    useKeep = true
  )

  val localAxisCfg = Axi4StreamConfig(
    dataWidth = busWidth / 8,
    destWidth = 6,
    useDest = true,
    useLast = true
  )

  val io = new Bundle {
    val tokenIndex = slave(Stream(util.AxiFrame(Bits(16 bits), userBit = 6)))
    val mm2s = slave(Axi4Stream(dataMoverAxisCfg))
    val s2mm = master(Axi4Stream(dataMoverAxisCfg))
    val mm2sCmd = master(Stream(Bits(72 bits)))
    val s2mmCmd = master(Stream(Bits(72 bits)))
  }

  val local = new Bundle {
    val bus = master(Axi4Stream(localAxisCfg))
    val kvBus = slave(Axi4Stream(localAxisCfg))
    val index = slave(Flow(Fragment(util.AxiFrame(Bits(16 bits), userBit = 6))))
  }

  val status = new Bundle {
    val enPredictor = in Bool()
  }

  import LLaMA2_7B._

  val token = UInt(log2Up(maxToken) bits).setAsReg().init(0)
  val tokenHigh = token.dropLow(log2Up(busWidth / 32)).asUInt
  val tokenLow = token.takeLow(log2Up(busWidth / 32))
  val firstToken = token === 0
  val noSzFromMem = tokenHigh === 0

  val mmap = new LLaMA2_7B_MMAP(
    dim = dim,
    mlpDim = mlpDim,
    predDim = predDim,
    layer = layer,
    head = head,
    vocabSize = vocabSize,
    numOfCore = numOfCore,
    busWidth = busWidth,
    maxToken = maxToken
  )

  val mallocPerHead = U(mmap.attnQKV_len)
  val headBase = UInt(32 bits).setAsReg().init(mmap.attnQKV_addr)
  val headBaseNext = UInt(32 bits)
  val enIncHead = Bool()
  val headCnt = UInt(log2Up(head / numOfCore) bits).setAsReg().init(0)
  val headCntAbout2Ovf = headCnt === head / numOfCore - 2
  val headCntOvf = Bool().setAsReg().init(False)
  headBase := headBaseNext
  headBaseNext := headBase
  when(enIncHead) {
    headCnt := headCnt + 1
    headBaseNext := headBase + mallocPerHead
    when(headCntAbout2Ovf)(headCntOvf.set())
    when(headCntOvf) {
      headCnt := 0
      headBaseNext := mmap.attnQKV_addr
      headCntOvf.clear()
    }
  }

  val mallocPerLayer = Mux(status.enPredictor, U(mmap.sparseLayer_len), U(mmap.denseLayer_len))
  val layerBase = UInt(32 bits).setAsReg().init(baseAddr + mmap.afterTokenizer_addr)
  val layerBaseNext = UInt(32 bits)
  val enIncLayer = Bool()
  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val layerCntAbout2Ovf = layerCnt === layer - 2
  val layerCntOvf = Bool().setAsReg().init(False)
  layerBase := layerBaseNext
  layerBaseNext := layerBase
  when(enIncLayer) {
    layerCnt := layerCnt + 1
    layerBaseNext := layerBase + mallocPerLayer
    when(layerCntAbout2Ovf)(layerCntOvf.set())
    when(layerCntOvf) {
      layerCnt := 0
      layerBaseNext := baseAddr + mmap.afterTokenizer_addr
      layerCntOvf.clear()
    }
  }

  val attnHeadBase = UInt(32 bits).setAsReg().init(0)
  val attnHeadBaseNext = layerBaseNext + headBaseNext
  attnHeadBase := attnHeadBaseNext

  val tokenIn = Stream(Fragment(Bits(72 bits)))
  tokenIn.arbitrationFrom(io.tokenIndex)
  tokenIn.last.set()
  tokenIn.payload := GenAxiDataMoverCmd(
    io.tokenIndex.tdata.asUInt * GenMemCmdLen.tokenIn(dim, numOfCore),
    U(GenMemCmdLen.tokenIn(dim, numOfCore)),
    U(mmap.vocabTable_addr + baseAddr),
    inc = True, eof = True
  )

  val tokenTag = Stream(Bits(6 bits))
  tokenTag.valid := tokenIn.fire
  tokenTag.payload := io.tokenIndex.tuser

  val attnLn = Stream(Fragment(Bits(72 bits)))
  attnLn.valid.set()
  attnLn.last.set()
//  attnLn.payload := GenAxiDataMoverCmd(U(mmap.attnLnScale_addr), U(mmap.attnLnScale_len), layerBase, inc = True, eof = True, tag = B"0011")
  attnLn.payload := GenAxiDataMoverCmd(U(mmap.attnLnScale_addr), U(mmap.attnLnScale_len), layerBase, inc = True, eof = True, tag = B"0000")

  val attnLnTag = Stream(Bits(6 bits))
  attnLnTag.valid := attnLn.fire
  attnLnTag.payload := B(param.ATTN_LN_SCALE, 6 bits)

  val attnQCmd = GenAxiDataMoverCmd(U(mmap.attnQ_addr), U(mmap.attnQ_len), attnHeadBase, inc = True, eof = True)
  val attnKCmd = GenAxiDataMoverCmd(U(mmap.attnK_addr), U(mmap.attnK_len), attnHeadBase, inc = True, eof = True)
  val attnVCmd = GenAxiDataMoverCmd(U(mmap.attnV_addr), U(mmap.attnV_len), attnHeadBase, inc = True, eof = True)
  val attnKCacheCmd = GenAxiDataMoverCmd(
    U(mmap.attnKCache_addr),
    GenMemCmdLen.kvCache(dim / head, token).asUInt,
    attnHeadBase,
    inc = True, eof = True, tag = B"0001"
  )
  val attnVCacheCmd = GenAxiDataMoverCmd(
    U(mmap.attnVCache_addr),
    GenMemCmdLen.kvCache(dim / head, token).asUInt,
    attnHeadBase,
    inc = True, eof = True, tag = B"0001"
  )
  val attnKszCmd = GenAxiDataMoverCmd(
    U(mmap.attnKScaleZero_addr),
    GenMemCmdLen.kvScaleZero(token, busWidth).asUInt,
    attnHeadBase,
    inc = True, eof = True, tag = B"0010"
  )
  val attnVszCmd = GenAxiDataMoverCmd(
    U(mmap.attnVScaleZero_addr),
    GenMemCmdLen.kvScaleZero(token, busWidth).asUInt,
    attnHeadBase,
    inc = True, eof = True, tag = B"0010"
  )

  def gen_split_cmd(addr: BigInt, len: BigInt, base: UInt, split: Int) = {
    val cmdBuf = ArrayBuffer[Bits]()
    val splitLen = len / split
    for (i <- 0 until split) {
      val cmd = GenAxiDataMoverCmd(U(addr + i * splitLen), U(splitLen), base, inc = True, eof = if (i == split - 1) True else False)
      cmdBuf.append(cmd)
    }
    Vec(cmdBuf.toArray)
  }

  val attnO = GenAxiDataMoverCmd(U(mmap.attnO_addr), U(mmap.attnO_len), layerBase, inc = True, eof = True)
//  val mlpLnCmd = GenAxiDataMoverCmd(U(mmap.mlpLnScale_addr), U(mmap.mlpLnScale_len), layerBase, inc = True, eof = True, tag = B"0011")
  val mlpLnCmd = GenAxiDataMoverCmd(U(mmap.mlpLnScale_addr), U(mmap.mlpLnScale_len), layerBase, inc = True, eof = True, tag = B"0000")
  val mlpPredUCmd = GenAxiDataMoverCmd(U(mmap.mlpPredU_addr), U(mmap.mlpPredU_len), layerBase, inc = True, eof = True)
  val mlpDenseGCmd = GenAxiDataMoverCmd(U(mmap.mlpDenseG_addr), U(mmap.mlpDenseG_len), layerBase, inc = True, eof = True)

  val attnO_0 = GenAxiDataMoverCmd(U(mmap.attnO_addr), U(mmap.attnO_len / 2), layerBase, inc = True, eof = False)
  val attnO_1 = GenAxiDataMoverCmd(U(mmap.attnO_addr + mmap.attnO_len / 2), U(mmap.attnO_len / 2), layerBase, inc = True, eof = True)

  //  val mlpDenseGCmd_0 = GenAxiDataMoverCmd(U(mmap.mlpDenseG_addr + 0 * mmap.mlpDenseG_len / 4), U(mmap.mlpDenseG_len / 4), layerBase, inc = True, eof = False)
  //  val mlpDenseGCmd_1 = GenAxiDataMoverCmd(U(mmap.mlpDenseG_addr + 1 * mmap.mlpDenseG_len / 4), U(mmap.mlpDenseG_len / 4), layerBase, inc = True, eof = False)
  //  val mlpDenseGCmd_2 = GenAxiDataMoverCmd(U(mmap.mlpDenseG_addr + 2 * mmap.mlpDenseG_len / 4), U(mmap.mlpDenseG_len / 4), layerBase, inc = True, eof = False)
  //  val mlpDenseGCmd_3 = GenAxiDataMoverCmd(U(mmap.mlpDenseG_addr + 3 * mmap.mlpDenseG_len / 4), U(mmap.mlpDenseG_len / 4), layerBase, inc = True, eof = True)
  val mlpDenseGCmdVec = gen_split_cmd(mmap.mlpDenseG_addr, mmap.mlpDenseG_len, layerBase, 4)

  val indexCmdGen = new Area {
    val sparseBaseAddrOs = Vec(U(mmap.mlpPredD_addr), U(mmap.mlpSparseG_addr), U(mmap.mlpSparseU_addr), U(mmap.mlpSparseD_addr))
    val sparseLen = Vec(U(mmap.mlpPredD_len), U(mmap.mlpSparseG_len), U(mmap.mlpSparseU_len), U(mmap.mlpSparseD_len))
    val sparseLastCnt = UInt(log2Up(sparseBaseAddrOs.length) bits).setAsReg().init(0)
    when(local.index.valid && local.index.last & status.enPredictor) {
      sparseLastCnt := sparseLastCnt + 1
      when(sparseLastCnt === sparseBaseAddrOs.length - 1) {
        sparseLastCnt := 0
      }
    }
    val denseBaseAddrOs = Vec(U(mmap.mlpDenseU_addr), U(mmap.mlpDenseD_addr))
    val denseLen = Vec(U(mmap.mlpDenseU_len), U(mmap.mlpDenseD_len))
    val denseTag = Vec(B"0100", B"0101")
    val denseLastCnt = UInt(log2Up(denseBaseAddrOs.length) bits).setAsReg().init(0)
    when(local.index.valid && local.index.last & ~status.enPredictor) {
      denseLastCnt := denseLastCnt + 1
      when(denseLastCnt === denseBaseAddrOs.length - 1) {
        denseLastCnt := 0
      }
    }
    val selSparseBaseAddr = sparseBaseAddrOs(sparseLastCnt)
    val selSparseLen = sparseLen(sparseLastCnt)
    val selDenseBaseAddr = denseBaseAddrOs(denseLastCnt)
    val selDenseLen = denseLen(denseLastCnt)
    val selDenseTag = denseTag(denseLastCnt)
    val selBaseAddr = RegNext(Mux(status.enPredictor, selSparseBaseAddr, selDenseBaseAddr), init = U(0))
    val selLen = RegNext(Mux(status.enPredictor, selSparseLen, selDenseLen), init = U(0))
    val selTag = RegNext(selDenseTag)

    val indexFlow = Flow(Fragment(UInt(16 bits)))
    indexFlow.valid := RegNext(local.index.valid, init = False)
    indexFlow.last := RegNext(local.index.last, init = False)
    indexFlow.fragment := RegNext(local.index.tdata.asUInt)

    //    val indexCmd = GenAxiDataMoverCmd.fromIndex(indexFlow, layerBase + selBaseAddr, selLen, inc = True)
    val indexCmd = GenAxiDataMoverCmd.fromIndexWithPackMerge(indexFlow, layerBase + selBaseAddr, selLen, tag = selTag, dmaSplit = dmaSplit)
    val indexCmdFifo = new StreamFifoPipe(Bits(72 bits), mlpDim, forFMax = true)
    indexCmdFifo.logic.ram.addAttribute("ram_style", "ultra")
    indexCmdFifo.io.push.valid := indexCmd.valid
    indexCmdFifo.io.push.payload := indexCmd.fragment
    indexCmdFifo.io.pop.ready.clear()

    val indexLastFifo = new StreamFifoPipe(Bool(), mlpDim, forFMax = true)
    indexLastFifo.io.push.valid := indexCmdFifo.io.push.fire
    indexLastFifo.io.push.payload := indexCmd.last
    indexLastFifo.io.pop.ready := indexCmdFifo.io.pop.fire
  }

  val lgSparseScale = GenAxiDataMoverCmd(U(mmap.lgSparseLnScale_addr), U(mmap.lgSparseLnScale_len), U(baseAddr), inc = True, eof = True)
  val lgSparseHead = GenAxiDataMoverCmd(U(mmap.lgSparseHead_addr), U(mmap.lgSparseHead_len), U(baseAddr), inc = True, eof = True)
  val lgDenseScale = GenAxiDataMoverCmd(U(mmap.lgDenseLnScale_addr), U(mmap.lgDenseLnScale_len), U(baseAddr), inc = True, eof = True)
  val lgDenseHead = GenAxiDataMoverCmd(U(mmap.lgDenseHead_addr), U(mmap.lgDenseHead_len), U(baseAddr), inc = True, eof = True)

  val lgSplit = 16
  val lgSparseHeadVec = gen_split_cmd(mmap.lgSparseHead_addr, mmap.lgSparseHead_len, U(baseAddr), lgSplit)
  val lgDenseHeadVec = gen_split_cmd(mmap.lgDenseHead_addr, mmap.lgDenseHead_len, U(baseAddr), lgSplit)

  val attnKV = new Area {
    val vec = Vec(attnKCmd, attnVCmd)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vec.length) bits)
    val cntOvfNext = cntNext === vec.length - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.last := cntOvf
    cmd.valid.set()
    cmd.fragment := vec(cnt)

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext := 0
      }
    }

    val tagVec = Vec(B(param.ATTN_W_K, 6 bits), B(param.ATTN_W_V, 6 bits))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire
  }

  val attnQKVNoSz = new Area {
    val vec = Vec(attnQCmd, attnKCmd, attnKCacheCmd, attnVCmd, attnVCacheCmd)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vec.length) bits)
    val cntOvfNext = cntNext === vec.length - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.last := cntOvf
    cmd.valid.set()
    cmd.fragment := vec(cnt)

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext := 0
      }
    }

    val tagVec = Vec(B(param.ATTN_W_Q, 6 bits), B(param.ATTN_W_K, 6 bits), B(param.ATTN_K_CACHE, 6 bits), B(param.ATTN_W_V, 6 bits), B(param.ATTN_V_CACHE, 6 bits))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire
  }

  val attnQKVWithSz = new Area {
    val vec = Vec(attnQCmd, attnKCmd, attnKszCmd, attnKCacheCmd, attnVCmd, attnVszCmd, attnVCacheCmd)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vec.length) bits)
    val cntOvfNext = cntNext === vec.length - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.last := cntOvf
    cmd.valid.set()
    cmd.fragment := vec(cnt)

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext := 0
      }
    }

    val tagVec = Vec(B(param.ATTN_W_Q, 6 bits), B(param.ATTN_W_K, 6 bits), B(param.ATTN_K_CACHE, 6 bits), B(param.ATTN_K_CACHE, 6 bits), B(param.ATTN_W_V, 6 bits), B(param.ATTN_V_CACHE, 6 bits), B(param.ATTN_V_CACHE, 6 bits))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire
  }

  val mlpSparse = new Area {
    val vec = Vec(attnO, mlpLnCmd, mlpPredUCmd, indexCmdGen.indexCmdFifo.io.pop.payload)
    val vecLen = 7
    val cnt = UInt(log2Up(vecLen) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vecLen) bits)
    val cntOvfNext = cntNext === vecLen - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.fragment := vec(Min(cnt, U(3)).resize(2))
    cmd.valid.set()
    cmd.last.set()
    when(cnt === 3 || cnt === 4 || cnt === 5 || cnt === 6) {
      cmd.valid := indexCmdGen.indexCmdFifo.io.pop.valid
      cmd.last := indexCmdGen.indexLastFifo.io.pop.payload
      indexCmdGen.indexCmdFifo.io.pop.ready := cmd.ready & status.enPredictor
    }

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire & cmd.last) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext := 0
      }
    }

    val tagVec = Vec(B(param.ATTN_W_O, 6 bits), B(param.MLP_LN_SCALE, 6 bits), B(param.MLP_PRED_W_U, 6 bits), B(param.MLP_PRED_W_D, 6 bits), B(param.MLP_W_G, 6 bits), B(param.MLP_W_U, 6 bits), B(param.MLP_W_D, 6 bits))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire & cmd.isFirst
  }

  val mlpDense = new Area {
    //    val vec = Vec(attnO, mlpLnCmd, mlpDenseGCmd, indexCmdGen.indexCmdFifo.io.pop.payload)
    //    val vec = Vec(attnO_0, attnO_1, mlpLnCmd, mlpDenseGCmd_0, mlpDenseGCmd_1, mlpDenseGCmd_2, mlpDenseGCmd_3, indexCmdGen.indexCmdFifo.io.pop.payload)
    val vec = Vec(attnO_0, attnO_1, mlpLnCmd) ++ mlpDenseGCmdVec ++ Vec(indexCmdGen.indexCmdFifo.io.pop.payload)
    val vecLen = 9
    val cnt = UInt(log2Up(vecLen) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vecLen) bits)
    val cntOvfNext = cntNext === vecLen - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.fragment := vec(Min(cnt, U(7)).resize(3))
    cmd.valid.set()
    cmd.last.set()
    when(cnt === 7 || cnt === 8) {
      cmd.valid := indexCmdGen.indexCmdFifo.io.pop.valid
      cmd.last := indexCmdGen.indexLastFifo.io.pop.payload
      indexCmdGen.indexCmdFifo.io.pop.ready := cmd.ready & ~status.enPredictor
    }

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire & cmd.last) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext := 0
      }
    }

    val tagVec = Vec(B(param.ATTN_W_O, 6 bits), B(param.ATTN_W_O, 6 bits), B(param.MLP_LN_SCALE, 6 bits), B(param.MLP_W_G, 6 bits), B(param.MLP_W_G, 6 bits), B(param.MLP_W_G, 6 bits), B(param.MLP_W_G, 6 bits), B(param.MLP_W_U, 6 bits), B(param.MLP_W_D, 6 bits))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire & cmd.isFirst & cnt =/= 1 & cnt =/= 4 & cnt =/= 5 & cnt =/= 6
  }

  val logits = new Area {
    //    val vec = Vec(
    //      Mux(status.enPredictor, lgSparseScale, lgDenseScale),
    //      Mux(status.enPredictor, lgSparseHead, lgDenseHead)
    //    )
    val vec = Vec(Mux(status.enPredictor, lgSparseScale, lgDenseScale)) ++ Mux(status.enPredictor, lgSparseHeadVec, lgDenseHeadVec)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntNext = UInt(log2Up(vec.length) bits)
    val cntOvfNext = cntNext === vec.length - 1
    val cntOvf = Bool().setAsReg().init(False)
    cntOvf := cntOvfNext
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.fragment := vec(cnt)
    cmd.valid.set()
    cmd.last := cntOvf

    cnt := cntNext
    cntNext := cnt
    when(cmd.fire) {
      cntNext := cnt + 1
      when(cntOvf) {
        cntNext.clearAll()
      }
    }

    //    val tagVec = Vec(B(param.OUT_LN_SCALE, 6 bits), B(param.LM_HEAD_W, 6 bits))
    val tagVec = Vec(Array(B(param.OUT_LN_SCALE, 6 bits)) ++ Array.fill(lgSplit)(B(param.LM_HEAD_W, 6 bits)))
    val tag = Stream(Bits(6 bits))
    tag.payload := tagVec(cnt)
    tag.valid := cmd.fire & (cnt === 0 || cnt === 1)
  }

  val kvDone = attnKV.cmd.fire & attnKV.cmd.last
  val qkvNoSzDone = attnQKVNoSz.cmd.fire & attnQKVNoSz.cmd.last
  val qkvDone = attnQKVWithSz.cmd.fire & attnQKVWithSz.cmd.last
  val sparseMlpDone = mlpSparse.cmd.fire & mlpSparse.cmd.last & mlpSparse.cntOvf
  val denseMlpDone = mlpDense.cmd.fire & mlpDense.cmd.last & mlpDense.cntOvf
  val mlpDone = sparseMlpDone || denseMlpDone
  val logitsDone = logits.cmd.fire & logits.cmd.last

  val mm2sCmdMux = new StreamMux(Fragment(Bits(72 bits)), 8)
  mm2sCmdMux.io.inputs(0) << tokenIn
  mm2sCmdMux.io.inputs(1) << attnLn
  mm2sCmdMux.io.inputs(2) << attnKV.cmd
  mm2sCmdMux.io.inputs(3) << attnQKVNoSz.cmd
  mm2sCmdMux.io.inputs(4) << attnQKVWithSz.cmd
  mm2sCmdMux.io.inputs(5) << mlpSparse.cmd
  mm2sCmdMux.io.inputs(6) << mlpDense.cmd
  mm2sCmdMux.io.inputs(7) << logits.cmd
  //  val lnScaleTagHit = mm2sCmdMux.io.output.fragment.takeHigh(8).take(4) === B"0011"
  //  val mm2sCmdMuxOut = mm2sCmdMux.io.output.throwWhen(lnScaleTagHit)
  val mm2sCmdMuxOut = mm2sCmdMux.io.output

  val busTagMux = new StreamMux(Bits(6 bits), 8)
  busTagMux.io.inputs(0) << tokenTag
  busTagMux.io.inputs(1) << attnLnTag
  busTagMux.io.inputs(2) << attnKV.tag
  busTagMux.io.inputs(3) << attnQKVNoSz.tag
  busTagMux.io.inputs(4) << attnQKVWithSz.tag
  busTagMux.io.inputs(5) << mlpSparse.tag
  busTagMux.io.inputs(6) << mlpDense.tag
  busTagMux.io.inputs(7) << logits.tag
  val lnScaleTagVec = Vec(B(param.ATTN_LN_SCALE, 6 bits), B(param.OUT_LN_SCALE, 6 bits), B(param.MLP_LN_SCALE, 6 bits))
  //  val busTagLnScaleHit = lnScaleTagVec.map(_ === busTagMux.io.output.payload).orR
  //  val busTagMuxOut = busTagMux.io.output.throwWhen(busTagLnScaleHit)
  val busTagMuxOut = busTagMux.io.output

  val tagFifo = StreamFifo(Bits(6 bits), 64, forFMax = true)
  //  tagFifo.io.push.arbitrationFrom(busTagMux.io.output)
  //  tagFifo.io.push.payload := busTagMux.io.output.payload
  tagFifo.io.push.arbitrationFrom(busTagMuxOut)
  tagFifo.io.push.payload := busTagMuxOut.payload

  local.bus.arbitrationFrom(io.mm2s)
  local.bus.data := io.mm2s.data
  local.bus.dest := tagFifo.io.pop.payload.take(6).asUInt
  local.bus.last := io.mm2s.last
  tagFifo.io.pop.ready := io.mm2s.fire & io.mm2s.last

  val prefill = Bool().setAsReg().init(True)
  when(tokenTag.fire & tokenTag.payload === extTokenTag._2) {
    prefill.clear()
  }

  io.s2mm.arbitrationFrom(local.kvBus)
  io.s2mm.data := local.kvBus.data
  io.s2mm.last := local.kvBus.last
  io.s2mm.keep.setAll()

  val mm2sCmd = Stream(Bits(72 bits))
  //  mm2sCmd.arbitrationFrom(mm2sCmdMux.io.output)
  //  mm2sCmd.payload := mm2sCmdMux.io.output.fragment
  mm2sCmd.arbitrationFrom(mm2sCmdMuxOut)
  mm2sCmd.payload := mm2sCmdMuxOut.fragment

  io.mm2sCmd << mm2sCmd.s2mPipe().m2sPipe()

  enIncHead := kvDone || qkvNoSzDone || qkvDone
  enIncLayer := mlpDone
  when(prefill & layerCntOvf) {
    enIncLayer := kvDone & headCntOvf
  }

  val enTokenCnt = enIncLayer & layerCntOvf
  when(enTokenCnt) {
    token := token + 1
  }

  val select = UInt(3 bits).setAsReg().init(0)
  val selectNext = UInt(3 bits)
  selectNext := select
  select := selectNext
  mm2sCmdMux.io.select := select
  busTagMux.io.select := select

  when(select === 0 & tokenIn.fire) {
    selectNext := 1
  }
  when(select === 1 & attnLn.fire) {
    when(firstToken || prefill & layerCntOvf) {
      selectNext := 2
    }.elsewhen(noSzFromMem) {
      selectNext := 3
    }.otherwise {
      selectNext := 4
    }
  }
  when(select === 2 & kvDone & headCntOvf) {
    when(layerCntOvf) {
      selectNext := 0
    }.otherwise {
      when(status.enPredictor) {
        selectNext := 5
      }.otherwise {
        selectNext := 6
      }
    }
  }
  when(select === 3 & qkvNoSzDone & headCntOvf) {
    when(status.enPredictor) {
      selectNext := 5
    }.otherwise {
      selectNext := 6
    }
  }
  when(select === 4 & qkvDone & headCntOvf) {
    when(status.enPredictor) {
      selectNext := 5
    }.otherwise {
      selectNext := 6
    }
  }
  when(select === 5 & sparseMlpDone) {
    when(layerCntOvf) {
      selectNext := 7
    }.otherwise {
      selectNext := 1
    }
  }
  when(select === 6 & denseMlpDone) {
    when(layerCntOvf) {
      selectNext := 7
    }.otherwise {
      selectNext := 1
    }
  }
  when(select === 7 & logitsDone) {
    selectNext := 0
  }


  val s2mm = new Area {

    val tokenEnFifo = new StreamFifo(NoData(), 32, forFMax = true)
    tokenEnFifo.io.push.valid := io.tokenIndex.fire
    tokenEnFifo.io.pop.ready.clear()

    //    val mallocPerHead = U(mmap.attnQKV_len)
    //    val s2mmHeadBase = UInt(32 bits).setAsReg().init(mmap.attnQKV_addr)
    //    val enIncHead = Bool()
    //    val s2mmHeadCnt = UInt(log2Up(head / numOfCore) bits).setAsReg().init(0)
    //    val headCntOvf = s2mmHeadCnt === head / numOfCore - 1
    //    when(enIncHead) {
    //      s2mmHeadCnt := s2mmHeadCnt + 1
    //      s2mmHeadBase := s2mmHeadBase + mallocPerHead
    //      when(headCntOvf) {
    //        s2mmHeadCnt := 0
    //        s2mmHeadBase := mmap.attnQKV_addr
    //      }
    //    }
    //
    //    val attnHeadBase = layerBase + s2mmHeadBase
    //
    //    val s2mmLayerInc = Bool()
    //    val s2mmLayerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
    //    val s2mmTokenCnt = UInt(log2Up(maxToken) bits).setAsReg().init(0)
    //    val layerCntOvf = s2mmLayerCnt === layer - 1
    //    when(s2mmLayerInc) {
    //      s2mmLayerCnt := s2mmLayerCnt + 1
    //      when(layerCntOvf) {
    //        s2mmLayerCnt := 0
    //        s2mmTokenCnt := s2mmTokenCnt + 1
    //        tokenEnFifo.io.pop.ready.set()
    //      }
    //    }

    val mallocPerHead = U(mmap.attnQKV_len)
    val headBase = UInt(32 bits).setAsReg().init(mmap.attnQKV_addr)
    val headBaseNext = UInt(32 bits)
    val enIncHead = Bool()
    val headCnt = UInt(log2Up(head / numOfCore) bits).setAsReg().init(0)
    val headCntAbout2Ovf = headCnt === head / numOfCore - 2
    val headCntOvf = Bool().setAsReg().init(False)
    headBase := headBaseNext
    headBaseNext := headBase
    when(enIncHead) {
      headCnt := headCnt + 1
      headBaseNext := headBase + mallocPerHead
      when(headCntAbout2Ovf)(headCntOvf.set())
      when(headCntOvf) {
        headCnt := 0
        headBaseNext := mmap.attnQKV_addr
        headCntOvf.clear()
      }
    }

    val mallocPerLayer = Mux(status.enPredictor, U(mmap.sparseLayer_len), U(mmap.denseLayer_len))
    val layerBase = UInt(32 bits).setAsReg().init(baseAddr + mmap.afterTokenizer_addr)
    val layerBaseNext = UInt(32 bits)
    val enIncLayer = Bool()
    val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
    val layerCntAbout2Ovf = layerCnt === layer - 2
    val layerCntOvf = Bool().setAsReg().init(False)
    val s2mmTokenCnt = UInt(log2Up(maxToken) bits).setAsReg().init(0)
    layerBase := layerBaseNext
    layerBaseNext := layerBase
    when(enIncLayer) {
      layerCnt := layerCnt + 1
      layerBaseNext := layerBase + mallocPerLayer
      when(layerCntAbout2Ovf)(layerCntOvf.set())
      when(layerCntOvf) {
        layerCnt := 0
        layerBaseNext := baseAddr + mmap.afterTokenizer_addr
        layerCntOvf.clear()
        s2mmTokenCnt := s2mmTokenCnt + 1
        tokenEnFifo.io.pop.ready.set()
      }
    }

    val attnHeadBase = UInt(32 bits).setAsReg().init(0)
    val attnHeadBaseNext = layerBaseNext + headBaseNext
    attnHeadBase := attnHeadBaseNext

    val s2mmTokenCntLow = s2mmTokenCnt.takeLow(log2Up(busWidth / 32))
    val s2mmTokenHigh = s2mmTokenCnt.dropLow(log2Up(busWidth / 32)).asUInt
    val s2mSzToMem = s2mmTokenCntLow.andR

    val kvSzLen = busWidth / 8
    val kvLen = dim / head
    val kCacheCmd = GenAxiDataMoverCmd(
      (s2mmTokenCnt ## B(0, log2Up(kvLen / dmaSplit) bits)).asUInt,
      U(kvLen),
      attnHeadBase + mmap.attnKCache_addr,
      inc = True, eof = True, tag = B"0001"
    )
    val vCacheCmd = GenAxiDataMoverCmd(
      (s2mmTokenCnt ## B(0, log2Up(kvLen / dmaSplit) bits)).asUInt,
      U(kvLen),
      attnHeadBase + mmap.attnVCache_addr,
      inc = True, eof = True, tag = B"0001"
    )
    val kSzCmd = GenAxiDataMoverCmd(
      (s2mmTokenHigh ## B(0, log2Up(kvSzLen / dmaSplit) bits)).asUInt,
      U(kvSzLen),
      attnHeadBase + mmap.attnKScaleZero_addr,
      inc = True, eof = True, tag = B"0010"
    )
    val vSzCmd = GenAxiDataMoverCmd(
      (s2mmTokenHigh ## B(0, log2Up(kvSzLen / dmaSplit) bits)).asUInt,
      U(kvSzLen),
      attnHeadBase + mmap.attnVScaleZero_addr,
      inc = True, eof = True, tag = B"0010"
    )

    val s2mmCmd = Stream(Bits(72 bits))
    val vec = Vec(kSzCmd, kCacheCmd, vSzCmd, vCacheCmd)
    val toMemSel = UInt(2 bits)
    s2mmCmd.payload := vec(toMemSel)
    s2mmCmd.valid := tokenEnFifo.io.pop.valid

    val cnt = UInt(2 bits).setAsReg().init(0)
    when(s2mmCmd.fire) {
      cnt := cnt + 1
    }

    enIncHead := s2mmCmd.fire & cnt.andR
    enIncLayer := enIncHead & headCntOvf

    toMemSel := cnt
    val s2mmCmdThrow = s2mmCmd.throwWhen(~s2mSzToMem & cnt(0) === False)

    val cmdFifo = new StreamFifo(Bits(72 bits), 32, forFMax = true)
    cmdFifo.io.push << s2mmCmdThrow
    io.s2mmCmd << cmdFifo.io.pop
  }

  //  headCnt.addAttribute("mark_debug", "true")
  //  layerCnt.addAttribute("mark_debug", "true")
  //  token.addAttribute("mark_debug", "true")
  //
  //  s2mm.headCnt.addAttribute("mark_debug", "true")
  //  s2mm.layerCnt.addAttribute("mark_debug", "true")
  //  s2mm.s2mmTokenCnt.addAttribute("mark_debug", "true")
}
