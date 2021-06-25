use "ponytest"
use "../BlockCache"
use "../OFFStreams"
use "Streams"
use "../Global"
use "files"

class iso _TestNewBlocksRecipe is UnitTest
  fun name(): String => "Testing New Block Recipe"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    t.expect_action("generated")
    let tests: _NewBlocksRecipeTester = _NewBlocksRecipeTester(t)




actor _NewBlocksRecipeTester
  let _t: TestHelper
  var _br: (NewBlocksRecipe[Standard] | None) = None
  let _arr: Array[Block[Standard]]
  new create(t: TestHelper) =>
    _t = t
    _arr = Array[Block[Standard]](4)
    try
      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/")?
      let bc: BlockCache[Standard] = NewBlockCache[Standard](DefaultConfig(), path)?
      let br: NewBlocksRecipe[Standard] = NewBlocksRecipe[Standard](bc)
      _br = br
      let readableNotify: ReadableNotify iso = object iso is ReadableNotify
        let test: _NewBlocksRecipeTester = this
        fun apply() =>
          test._start()
      end
      let dataNotify: DataNotify[Block[Standard]] iso = object iso is DataNotify[Block[Standard]]
        let test: _NewBlocksRecipeTester = this
        fun apply(block: Block[Standard]) =>
          test._receiveBlock(block)
      end
      br.subscribe(consume dataNotify)
      br.subscribe(consume readableNotify)
    else
      t.fail("Block Cache Creation Error")
      t.complete(true)
    end
  be _start() =>
    match _br
      | let br: NewBlocksRecipe[Standard] =>
        br.pull()
    end


  be _receiveBlock(block: Block[Standard]) =>
    _arr.push(block)
    match _br
      | let br: NewBlocksRecipe[Standard] =>
        if _arr.size() < 4 then
            br.pull()
        else
          br.close()
          _runTest()
        end
    end

  fun _runTest() =>
    _t.complete_action("generated")
    _t.complete(true)
