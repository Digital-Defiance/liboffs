use "ponytest"
use "../BlockCache"
use "../OFFStreams"
use "Streams"
use "../Global"
use "files"
use "Exception"

class iso _TestNewBlocksRecipe is UnitTest
  fun name(): String => "Testing New Block Recipe"
  fun exclusion_group(): String => "Block Cache"
  fun ref set_up(t: TestHelper) =>
    try
      let offDir = Directory(FilePath(t.env.root, "offs/"))?
      offDir.remove("blocks")
    end
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    t.expect_action("generated")
    let tests: _NewBlocksRecipeTester[Standard] = _NewBlocksRecipeTester[Standard](t)

actor _NewBlocksRecipeTester[B: BlockType]
  let _t: TestHelper
  var _br: (NewBlocksRecipe[B] | None) = None
  let _arr: Array[Block[B]]
  new create(t: TestHelper) =>
    _t = t
    _arr = Array[Block[B]](4)
    try
      let path: FilePath = FilePath(t.env.root, "offs/blocks/")
      let bc: BlockCache[B] = NewBlockCache[B](DefaultConfig(), path)?
      let br: NewBlocksRecipe[B] = NewBlocksRecipe[B](bc)
      _br = br
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _NewBlocksRecipeTester[B] = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end
      let readableNotify: ReadableNotify iso = object iso is ReadableNotify
        let test: _NewBlocksRecipeTester[B]  = this
        fun apply() =>
          test._start()
      end
      let dataNotify: DataNotify[Block[B]] iso = object iso is DataNotify[Block[B]]
        let test: _NewBlocksRecipeTester[B]  = this
        fun apply(block: Block[B]) =>
          test._receiveBlock(block)
      end
      let closeNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _NewBlocksRecipeTester[B] = this
        fun apply() =>
          test._runTest()
      end
      br.subscribe(consume dataNotify)
      br.subscribe(consume readableNotify)
      br.subscribe(consume closeNotify)
      br.subscribe(consume errorNotify)
    else
      t.fail("Block Cache Creation Error")
      t.complete(true)
    end
  be _start() =>
    match _br
      | let br: NewBlocksRecipe[B] =>
        br.pull()
    end


  be _receiveBlock(block: Block[B]) =>
    _arr.push(block)
    match _br
      | let br: NewBlocksRecipe[B] =>
        if _arr.size() < 4 then
            br.pull()
        else
          br.close()
        end
    end

  be _runTest() =>
    _t.complete_action("generated")
    _t.complete(true)
