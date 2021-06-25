use "ponytest"
use "../BlockCache"
use "../OFFStreams"
use "Streams"
use "../Global"
use "files"
use "Exception"
use "collections"
use "time"
use "random"

class iso _TestRandomPopularityRecipe is UnitTest
  fun name(): String => "Testing Random Popularity Block Recipe"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(10000000000)
    t.expect_action("generated")
    let tests: _RandomPopularityRecipeTester = _RandomPopularityRecipeTester(t)

actor _BlockPopularityGenerator[B: BlockType]
  let _blocks: Array[Block[B]]
  let _bc: BlockCache[B]
  var _i: USize = 0
  var _j: USize = 0
  var _fibCount: U64
  let _t: TestHelper
  let _gen: Rand
  let _cb: {()} val

  new create(cb:{()} val, t: TestHelper, bc: BlockCache[B], count: USize = 4) =>
    _t = t
    _cb = cb
    _bc = bc
    let now = Time.now()
    _gen = Rand(now._1.u64(), now._2.u64())
    _fibCount = 100
    let bs: BlockService[B] = BlockService[B]
    _blocks = Array[Block[B]](count)
    try
      for i in Range(0, count) do
        _blocks.push(bs.newBlock()?)
      end
      _putBlocks()
    else
      _t.fail("Block Generation Error")
      _t.complete(true)
    end

  be _putBlocks() =>
    if _i < _blocks.size() then
      try
        _bc.put(_blocks(_i = _i + 1)?, {(err: (None | SectionWriteError )) (bpg: _BlockPopularityGenerator[B] = this) =>
          match err
            | None =>
              bpg._putBlocks()
            | SectionWriteError =>
              _t.fail("Section Write Error")
              _t.complete(true)
          end
        })
      else
        _t.fail("Block Error")
        _t.complete(true)
      end
    else
      _i = 0
      _j = 0
      _generatePopularity()
    end

  be _generatePopularity() =>
    if _i < _blocks.size() then
      if _j < _fibCount.usize() then
        try
          _bc.get(_blocks(_i)?.hash, {(block: (Block[B] | SectionReadError | BlockNotFound)) (bpg: _BlockPopularityGenerator[B] = this) =>
            match block
              | let block': Block[B] =>
                bpg._generatePopularity()
            else
              _t.fail("Retrieval Error")
              _t.complete(true)
            end
          })
          _j = _j + 1
        else
          _t.fail("Block Error")
          _t.complete(true)
        end
      else
        _i = _i + 1
        _j = 0
        _fibCount = 1000
        _generatePopularity()
      end
    else
      _cb()
    end




actor _RandomPopularityRecipeTester
  let _t: TestHelper
  var _br: (RandomPopularityRecipe[Standard] | None) = None
  let _arr: Array[Block[Standard]]
  var _bpg: (_BlockPopularityGenerator[Standard] | None) = None
  new create(t: TestHelper) =>
    _t = t
    _arr = Array[Block[Standard]](4)
    try
      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/")?
      let bc: BlockCache[Standard] = NewBlockCache[Standard](DefaultConfig(), path)?
      let br: RandomPopularityRecipe[Standard] = RandomPopularityRecipe[Standard](bc)
      _br = br
      let readableNotify: ReadableNotify iso = object iso is ReadableNotify
        let test: _RandomPopularityRecipeTester = this
        fun apply() => None
      end
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _RandomPopularityRecipeTester = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end
      let dataNotify: DataNotify[Block[Standard]] iso = object iso is DataNotify[Block[Standard]]
        let test: _RandomPopularityRecipeTester = this
        fun apply(block: Block[Standard]) =>
          test._receiveBlock(block)
      end
      let closeNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _RandomPopularityRecipeTester = this
        fun apply() =>
          test._runTest()
      end
      br.subscribe(consume errorNotify)
      br.subscribe(consume dataNotify)
      br.subscribe(consume readableNotify)
      br.subscribe(consume closeNotify)
      let cb = {() (_rt: _RandomPopularityRecipeTester tag = this) =>
        _rt._start()
      } val
      _bpg = _BlockPopularityGenerator[Standard](cb, t, bc, 1)
    else
      t.fail("Block Cache Creation Error")
      t.complete(true)
    end
  be _start() =>
    match _br
      | let br: RandomPopularityRecipe[Standard] =>
        br.pull()
    end


  be _receiveBlock(block: Block[Standard]) =>
    _arr.push(block)
    match _br
      | let br: RandomPopularityRecipe[Standard] =>
        if _arr.size() < 4 then
            br.pull()
        else
          br.close()
        end
    end

  be _runTest() =>
    _t.complete_action("generated")
    _t.complete(true)
