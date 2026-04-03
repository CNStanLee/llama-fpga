package cfgGen

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axis._
import util.{GenAxiDataMoverCmd, StreamFifoPipe}

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class GenMemCmdSmallBurst(
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

  def gen_split_cmd(addr: BigInt, len: BigInt, base: UInt, split: Int) = {
    val cmdBuf = ArrayBuffer[Bits]()
    val splitLen = len / split
    for (i <- 0 until split) {
      val cmd = GenAxiDataMoverCmd(U(addr + i * splitLen), U(splitLen), base, inc = True, eof = if (i == split - 1) True else False)
      cmdBuf.append(cmd)
    }
    Vec(cmdBuf.toArray)
  }

  def gen_split_cmd_stream(addr: BigInt, len: BigInt, base: UInt, split: Int) = {
    if (split == 1) {
      val cmd = GenAxiDataMoverCmd(addr, U(len), base, inc = True, eof = True)
      val ret = Stream(Fragment(Bits(72 bits)))
      ret.valid.set()
      ret.fragment := cmd
      ret.last.set()
      ret
    }
    else {
      val offset = UInt(32 bits).setAsReg().init(addr)
      val splitLen = len / split
      println(splitLen)
      val cnt = UInt(log2Up(split) bits).setAsReg().init(0)
      val offsetInc = Bool()
      val cntOvf = cnt === split - 1
      when(offsetInc) {
        cnt := cnt + 1
        offset := offset + splitLen
        when(cntOvf) {
          cnt.clearAll()
          offset := addr
        }
      }
      val cmd = GenAxiDataMoverCmd(offset, U(splitLen), base, inc = True, eof = cntOvf)
      val ret = Stream(Fragment(Bits(72 bits)))
      ret.valid.set()
      ret.fragment := cmd
      ret.last := cntOvf
      offsetInc := ret.fire
      ret
    }
  }

  //  def gen_split_cmd_stream(addr: BigInt, len: BigInt, base: UInt, cmdCnt: UInt, splitLen: UInt) = {
  //    val offset = UInt(32 bits).setAsReg().init(addr)
  //    val cnt = UInt(8 bits).setAsReg().init(0)
  //    val offsetInc = Bool()
  //    val cntOvf = cnt === cmdCnt
  //    when(offsetInc) {
  //      cnt := cnt + 1
  //      offset := offset + splitLen
  //      when(cntOvf) {
  //        cnt.clearAll()
  //        offset := addr
  //      }
  //    }
  //    val cmd = GenAxiDataMoverCmd(offset, splitLen, base, inc = True, eof = cntOvf)
  //    val ret = Stream(Fragment(Bits(72 bits)))
  //    ret.valid.set()
  //    ret.fragment := cmd
  //    ret.last := cntOvf
  //    offsetInc := ret.fire
  //    ret
  //  }

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

  //  val attnQKVSplit = 2048 / 64
  //  val attnOSplit = 32 * 2048 / 64
  //  val mlpDenseGSplit = 4096 / 64
  //  val lgSplit = 4096 / 64

  val attnQKVSplit = 2
  val attnOSplit = 2
  val mlpDenseGSplit = 4
  val lgSplit = 16

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
  attnLn.payload := GenAxiDataMoverCmd(U(mmap.attnLnScale_addr), U(mmap.attnLnScale_len), layerBase, inc = True, eof = True, tag = B"0000")

  val attnLnTag = Stream(Bits(6 bits))
  attnLnTag.valid := attnLn.fire
  attnLnTag.payload := B(param.ATTN_LN_SCALE, 6 bits)

  println("attnQ_len", mmap.attnQ_len)
  println("attnO_len", mmap.attnO_len)
  println("mlpPredU_len", mmap.mlpPredU_len)
  println("mlpDenseG_len", mmap.mlpDenseG_len)
  println("lgDenseHead_len", mmap.lgDenseHead_len)

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

    val indexCmd = GenAxiDataMoverCmd.fromIndexWithPackMerge(indexFlow, layerBase + selBaseAddr, selLen, tag = selTag, dmaSplit = dmaSplit)
    val indexCmdFifo = new StreamFifoPipe(Bits(72 bits), 12288, forFMax = true)
    indexCmdFifo.logic.ram.addAttribute("ram_style", "ultra")
    indexCmdFifo.io.push.valid := indexCmd.valid
    indexCmdFifo.io.push.payload := indexCmd.fragment

    //    val indexReady = Bool()
    //    indexReady := indexCmdFifo.io.push.ready
    //    indexReady.addAttribute("mark_debug", "true")

    val indexLastFifo = new StreamFifoPipe(Bool(), 12288, forFMax = true)
    indexLastFifo.io.push.valid := indexCmdFifo.io.push.fire
    indexLastFifo.io.push.payload := indexCmd.last
    indexLastFifo.io.pop.ready := indexCmdFifo.io.pop.fire

    //    val cmdCnt = UInt(16 bits).setAsReg().init(0)
    //    val cmdCntLock = UInt(16 bits).setAsReg().init(0)
    //    cmdCntLock.addAttribute("mark_debug", "true")
    //    when(indexCmd.valid){
    //      cmdCnt := cmdCnt + 1
    //      when(indexCmd.last){
    //        cmdCntLock := cmdCnt
    //        cmdCnt.clearAll()
    //      }
    //    }

    val indexCmdOut = Stream(Fragment(Bits(72 bits)))
    indexCmdOut.arbitrationFrom(indexCmdFifo.io.pop)
    indexCmdOut.fragment := indexCmdFifo.io.pop.payload
    indexCmdOut.last := indexLastFifo.io.pop.payload

    val deMux = new StreamDemux(Fragment(Bits(72 bits)), 2)
    deMux.io.input << indexCmdOut
    val toMlpDense = deMux.io.outputs(0)
    val toMlpPredict = deMux.io.outputs(1)
    deMux.io.select := status.enPredictor.asUInt
  }

  val attnKV = new Area {
    val attnKCmdVec = gen_split_cmd_stream(mmap.attnK_addr, mmap.attnK_len, attnHeadBase, attnQKVSplit)
    val attnVCmdVec = gen_split_cmd_stream(mmap.attnV_addr, mmap.attnV_len, attnHeadBase, attnQKVSplit)
    val ports = 2
    val mux = new StreamMux(Fragment(Bits(72 bits)), ports)
    mux.io.inputs(0) << attnKCmdVec
    mux.io.inputs(1) << attnVCmdVec
    val sel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val muxOutFire = mux.io.output.fire
    val muxOutIsFirst = mux.io.output.isFirst
    val selOvf = sel === ports - 1
    mux.io.select := sel
    when(muxOutFire & mux.io.output.last) {
      sel := sel + 1
      when(selOvf) {
        sel.clearAll()
      }
    }

    val tagVec = Vec(B(param.ATTN_W_K, 6 bits), B(param.ATTN_W_V, 6 bits))
    val tagSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val tagFire = muxOutFire & muxOutIsFirst
    when(muxOutFire & muxOutIsFirst) {
      tagSel := tagSel + 1
      when(tagSel === ports - 1) {
        tagSel.clearAll()
      }
    }
    val tag = Stream(Bits(6 bits))
    tag.valid := tagFire
    tag.payload := tagVec(tagSel)
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.arbitrationFrom(mux.io.output)
    cmd.fragment := mux.io.output.fragment
    cmd.last := mux.io.output.last & selOvf
  }

  val attnQKVNoSz = new Area {
    val attnQCmdVec = gen_split_cmd_stream(mmap.attnQ_addr, mmap.attnQ_len, attnHeadBase, attnQKVSplit)
    val attnKCmdVec = gen_split_cmd_stream(mmap.attnK_addr, mmap.attnK_len, attnHeadBase, attnQKVSplit)
    val attnVCmdVec = gen_split_cmd_stream(mmap.attnV_addr, mmap.attnV_len, attnHeadBase, attnQKVSplit)
    val attnKCacheCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnKCache_addr),
      GenMemCmdLen.kvCache(dim / head, token).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0001"
    )
    val attnVCacheCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnVCache_addr),
      GenMemCmdLen.kvCache(dim / head, token).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0001"
    )

    val ports = 5
    val mux = new StreamMux(Fragment(Bits(72 bits)), ports)
    mux.io.inputs(0) << attnQCmdVec
    mux.io.inputs(1) << attnKCmdVec
    mux.io.inputs(2) << attnKCacheCmd
    mux.io.inputs(3) << attnVCmdVec
    mux.io.inputs(4) << attnVCacheCmd
    val sel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val muxOutFire = mux.io.output.fire
    val muxOutIsFirst = mux.io.output.isFirst
    val selOvf = sel === ports - 1
    mux.io.select := sel
    when(muxOutFire & mux.io.output.last) {
      sel := sel + 1
      when(selOvf) {
        sel.clearAll()
      }
    }

    val tagVec = Vec(
      B(param.ATTN_W_Q, 6 bits),
      B(param.ATTN_W_K, 6 bits),
      B(param.ATTN_K_CACHE, 6 bits),
      B(param.ATTN_W_V, 6 bits),
      B(param.ATTN_V_CACHE, 6 bits)
    )
    val tagSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val tagFire = muxOutFire & muxOutIsFirst
    when(muxOutFire & muxOutIsFirst) {
      tagSel := tagSel + 1
      when(tagSel === ports - 1) {
        tagSel.clearAll()
      }
    }
    val tag = Stream(Bits(6 bits))
    tag.valid := tagFire
    tag.payload := tagVec(tagSel)
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.arbitrationFrom(mux.io.output)
    cmd.fragment := mux.io.output.fragment
    cmd.last := mux.io.output.last & selOvf
  }

  val attnQKVWithSz = new Area {
    val attnQCmdVec = gen_split_cmd_stream(mmap.attnQ_addr, mmap.attnQ_len, attnHeadBase, attnQKVSplit)
    val attnKCmdVec = gen_split_cmd_stream(mmap.attnK_addr, mmap.attnK_len, attnHeadBase, attnQKVSplit)
    val attnVCmdVec = gen_split_cmd_stream(mmap.attnV_addr, mmap.attnV_len, attnHeadBase, attnQKVSplit)
    val attnKszCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnKScaleZero_addr),
      GenMemCmdLen.kvScaleZero(token, busWidth).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0010"
    )
    val attnVszCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnVScaleZero_addr),
      GenMemCmdLen.kvScaleZero(token, busWidth).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0010"
    )
    val attnKCacheCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnKCache_addr),
      GenMemCmdLen.kvCache(dim / head, token).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0001"
    )
    val attnVCacheCmd = GenAxiDataMoverCmd.retStream(
      U(mmap.attnVCache_addr),
      GenMemCmdLen.kvCache(dim / head, token).asUInt,
      attnHeadBase,
      inc = True, eof = True, tag = B"0001"
    )

    val ports = 7
    val mux = new StreamMux(Fragment(Bits(72 bits)), ports)
    mux.io.inputs(0) << attnQCmdVec
    mux.io.inputs(1) << attnKCmdVec
    mux.io.inputs(2) << attnKszCmd
    mux.io.inputs(3) << attnKCacheCmd
    mux.io.inputs(4) << attnVCmdVec
    mux.io.inputs(5) << attnVszCmd
    mux.io.inputs(6) << attnVCacheCmd
    val sel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val muxOutFire = mux.io.output.fire
    val muxOutIsFirst = mux.io.output.isFirst
    val selOvf = sel === ports - 1
    mux.io.select := sel
    when(muxOutFire & mux.io.output.last) {
      sel := sel + 1
      when(selOvf) {
        sel.clearAll()
      }
    }

    val tagVec = Vec(
      B(param.ATTN_W_Q, 6 bits),
      B(param.ATTN_W_K, 6 bits),
      B(param.ATTN_K_CACHE, 6 bits),
      B(param.ATTN_K_CACHE, 6 bits),
      B(param.ATTN_W_V, 6 bits),
      B(param.ATTN_V_CACHE, 6 bits),
      B(param.ATTN_V_CACHE, 6 bits)
    )
    val tagSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val tagFire = muxOutFire & muxOutIsFirst
    when(muxOutFire & muxOutIsFirst) {
      tagSel := tagSel + 1
      when(tagSel === ports - 1) {
        tagSel.clearAll()
      }
    }
    val tag = Stream(Bits(6 bits))
    tag.valid := tagFire
    tag.payload := tagVec(tagSel)
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.arbitrationFrom(mux.io.output)
    cmd.fragment := mux.io.output.fragment
    cmd.last := mux.io.output.last & selOvf
  }

  val mlpWithPredict = new Area {
    //    val mlpPredUSplit = 1
    //    val mlpLnCmd = GenAxiDataMoverCmd.retStream(U(mmap.mlpLnScale_addr), U(mmap.mlpLnScale_len), layerBase, inc = True, eof = True, tag = B"0000")
    //    val attnOCmdVec = gen_split_cmd_stream(mmap.attnO_addr, mmap.attnO_len, layerBase, attnOSplit)
    //    val mlpPredUCmd = gen_split_cmd_stream(mmap.mlpPredU_addr, mmap.mlpPredU_len, layerBase, mlpPredUSplit)
    //
    //    val ports = 4
    //    val indexCmdType = 4
    //    val tagCnt = ports + indexCmdType - 1
    //    val mux = new StreamMux(Fragment(Bits(72 bits)), ports)
    //    mux.io.inputs(0) << attnOCmdVec
    //    mux.io.inputs(1) << mlpLnCmd
    //    mux.io.inputs(2) << mlpPredUCmd
    //    mux.io.inputs(3) << indexCmdGen.toMlpPredict
    //    val sel = UInt(log2Up(tagCnt) bits).setAsReg().init(0)
    //    val muxOutFire = mux.io.output.fire
    //    val muxOutIsFirst = mux.io.output.isFirst
    //    val selOvf = sel === tagCnt - 1
    //    val muxSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    //    mux.io.select := muxSel
    //    when(muxOutFire & mux.io.output.last) {
    //      sel := sel + 1
    //      when(muxSel =/= ports - 1) {
    //        muxSel := muxSel + 1
    //      }
    //      when(selOvf) {
    //        sel.clearAll()
    //        muxSel.clearAll()
    //      }
    //    }
    //
    //    val tagVec = Vec(
    //      B(param.ATTN_W_O, 6 bits),
    //      B(param.MLP_LN_SCALE, 6 bits),
    //      B(param.MLP_PRED_W_U, 6 bits),
    //      B(param.MLP_PRED_W_D, 6 bits),
    //      B(param.MLP_W_G, 6 bits),
    //      B(param.MLP_W_U, 6 bits),
    //      B(param.MLP_W_D, 6 bits)
    //    )
    //    val tagSel = UInt(log2Up(tagCnt) bits).setAsReg().init(0)
    //    val tagFire = muxOutFire & muxOutIsFirst
    //    when(muxOutFire & muxOutIsFirst) {
    //      tagSel := tagSel + 1
    //      when(tagSel === tagCnt - 1) {
    //        tagSel.clearAll()
    //      }
    //    }
    //    val tag = Stream(Bits(6 bits))
    //    tag.valid := tagFire
    //    tag.payload := tagVec(tagSel)
    //    val cmd = Stream(Fragment(Bits(72 bits)))
    //    cmd.arbitrationFrom(mux.io.output)
    //    cmd.fragment := mux.io.output.fragment
    //    cmd.last := mux.io.output.last & selOvf

    indexCmdGen.toMlpPredict.freeRun()
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.valid.clear()
    cmd.payload.clearAll()
    val tag = Stream(Bits(6 bits))
    tag.valid.clear()
    tag.payload.clearAll()
  }

  val mlpDense = new Area {
    val mlpLnCmd = GenAxiDataMoverCmd.retStream(U(mmap.mlpLnScale_addr), U(mmap.mlpLnScale_len), layerBase, inc = True, eof = True, tag = B"0000")
    val attnOCmdVec = gen_split_cmd_stream(mmap.attnO_addr, mmap.attnO_len, layerBase, attnOSplit)
    val mlpDenseGCmdVec = gen_split_cmd_stream(mmap.mlpDenseG_addr, mmap.mlpDenseG_len, layerBase, mlpDenseGSplit)

    val ports = 4
    val indexCmdType = 2
    val tagCnt = ports + indexCmdType - 1
    val mux = new StreamMux(Fragment(Bits(72 bits)), ports)
    mux.io.inputs(0) << attnOCmdVec
    mux.io.inputs(1) << mlpLnCmd
    mux.io.inputs(2) << mlpDenseGCmdVec
    mux.io.inputs(3) << indexCmdGen.toMlpDense
    val sel = UInt(log2Up(tagCnt) bits).setAsReg().init(0)
    val muxOutFire = mux.io.output.fire
    val muxOutIsFirst = mux.io.output.isFirst
    val selOvf = sel === tagCnt - 1
    val muxSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    mux.io.select := muxSel
    when(muxOutFire & mux.io.output.last) {
      when(muxSel =/= ports - 1) {
        muxSel := muxSel + 1
      }
      sel := sel + 1
      when(selOvf) {
        sel.clearAll()
        muxSel.clearAll()
      }
    }

    val tagVec = Vec(
      B(param.ATTN_W_O, 6 bits),
      B(param.MLP_LN_SCALE, 6 bits),
      B(param.MLP_W_G, 6 bits),
      B(param.MLP_W_U, 6 bits),
      B(param.MLP_W_D, 6 bits)
    )
    val tagSel = UInt(log2Up(tagCnt) bits).setAsReg().init(0)
    val tagFire = muxOutFire & muxOutIsFirst
    when(muxOutFire & muxOutIsFirst) {
      tagSel := tagSel + 1
      when(tagSel === tagCnt - 1) {
        tagSel.clearAll()
      }
    }
    val tag = Stream(Bits(6 bits))
    tag.valid := tagFire
    tag.payload := tagVec(tagSel)
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.arbitrationFrom(mux.io.output)
    cmd.fragment := mux.io.output.fragment
    cmd.last := mux.io.output.last & selOvf
  }

  val logits = new Area {

    //    val lgSparseScale = GenAxiDataMoverCmd.retStream(U(mmap.lgSparseLnScale_addr), U(mmap.lgSparseLnScale_len), U(baseAddr), inc = True, eof = True)
    //    val lgSparseHeadVec = gen_split_cmd_stream(mmap.lgSparseHead_addr, mmap.lgSparseHead_len, U(baseAddr), lgSplit)
    val lgDenseScale = GenAxiDataMoverCmd.retStream(U(mmap.lgDenseLnScale_addr), U(mmap.lgDenseLnScale_len), U(baseAddr), inc = True, eof = True)
    val lgDenseHeadVec = gen_split_cmd_stream(mmap.lgDenseHead_addr, mmap.lgDenseHead_len, U(baseAddr), lgSplit)

    val ports = 2
    val mux = new StreamMux(Fragment(Bits(72 bits)), ports * 2)
    mux.io.inputs(0) << lgDenseScale
    mux.io.inputs(1) << lgDenseHeadVec
    //    mux.io.inputs(2) << lgSparseScale
    //    mux.io.inputs(3) << lgSparseHeadVec
    mux.io.inputs(2).valid.clear()
    mux.io.inputs(2).payload.clearAll()
    mux.io.inputs(3).valid.clear()
    mux.io.inputs(3).payload.clearAll()

    val sel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val muxOutFire = mux.io.output.fire
    val muxOutIsFirst = mux.io.output.isFirst
    val selOvf = sel === ports - 1
    mux.io.select := (status.enPredictor ## sel).asUInt
    when(muxOutFire & mux.io.output.last) {
      sel := sel + 1
      when(sel === ports - 1) {
        sel.clearAll()
      }
    }

    val tagVec = Vec(B(param.OUT_LN_SCALE, 6 bits), B(param.LM_HEAD_W, 6 bits))
    val tagSel = UInt(log2Up(ports) bits).setAsReg().init(0)
    val tagFire = muxOutFire & muxOutIsFirst
    when(muxOutFire & muxOutIsFirst) {
      tagSel := tagSel + 1
      when(tagSel === ports - 1) {
        tagSel.clearAll()
      }
    }
    val tag = Stream(Bits(6 bits))
    tag.valid := tagFire
    tag.payload := tagVec(tagSel)
    val cmd = Stream(Fragment(Bits(72 bits)))
    cmd.arbitrationFrom(mux.io.output)
    cmd.fragment := mux.io.output.fragment
    cmd.last := mux.io.output.last & selOvf
  }

  val kvDone = attnKV.cmd.fire & attnKV.cmd.last
  val qkvNoSzDone = attnQKVNoSz.cmd.fire & attnQKVNoSz.cmd.last
  val qkvDone = attnQKVWithSz.cmd.fire & attnQKVWithSz.cmd.last
  val sparseMlpDone = mlpWithPredict.cmd.fire & mlpWithPredict.cmd.last
  val denseMlpDone = mlpDense.cmd.fire & mlpDense.cmd.last
  val mlpDone = sparseMlpDone || denseMlpDone
  val logitsDone = logits.cmd.fire & logits.cmd.last

  val mm2sCmdMux = new StreamMux(Fragment(Bits(72 bits)), 8)
  mm2sCmdMux.io.inputs(0) << tokenIn
  mm2sCmdMux.io.inputs(1) << attnLn
  mm2sCmdMux.io.inputs(2) << attnKV.cmd
  mm2sCmdMux.io.inputs(3) << attnQKVNoSz.cmd
  mm2sCmdMux.io.inputs(4) << attnQKVWithSz.cmd
  mm2sCmdMux.io.inputs(5) << mlpWithPredict.cmd
  mm2sCmdMux.io.inputs(6) << mlpDense.cmd
  mm2sCmdMux.io.inputs(7) << logits.cmd
  val mm2sCmdMuxOut = mm2sCmdMux.io.output

  val busTagMux = new StreamMux(Bits(6 bits), 8)
  busTagMux.io.inputs(0) << tokenTag
  busTagMux.io.inputs(1) << attnLnTag
  busTagMux.io.inputs(2) << attnKV.tag
  busTagMux.io.inputs(3) << attnQKVNoSz.tag
  busTagMux.io.inputs(4) << attnQKVWithSz.tag
  busTagMux.io.inputs(5) << mlpWithPredict.tag
  busTagMux.io.inputs(6) << mlpDense.tag
  busTagMux.io.inputs(7) << logits.tag
  val busTagMuxOut = busTagMux.io.output

  val tagFifo = new StreamFifo(Bits(6 bits), 64, forFMax = true)
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
  select.addAttribute("max_fanout", 100)
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
}
