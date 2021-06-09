use "ponytest"
use "../BlockCache"
use "../OFFStreams"
use "Streams"

class iso _TestNewBlocksRecipe is UnitTest
  fun name(): String => "Testing New Block Recipe"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    t.expect_action("generated")
    let tests: _NewBlocksRecipeTester = _NewBlocksRecipeTester(t)




actor _NewBlocksRecipeTester
  let _t: TestHelper
  let _br: NewBlocksRecipe[Standard]
  let _arr: Array[Block[Standard]]
  new create(t: TestHelper) =>
    _t = t
    _br = NewBlocksRecipe[Standard]
    _arr = Array[Block[Standard]](4)
    let readableNotify: ReadableNotify iso = object iso is ReadableNotify
      let test: _TestNewBlockRecipe = this
      fun apply() =>
        test._start()
    end
    let dataNotify: DataNotify[Block[Standard]]= object iso is DataNotify[Block[Standard]]
      let test: _TestNewBlockRecipe = this
      fun apply(block: Block[Standard]) =>
        test._receiveBlock(data)
    end
    _br.subscribe(dataNotify)
    _br.subscribe(readableNotify)
  be start() =>
    _br.pull()

  be _receiveBlock(block: Block[Standard]) =>
    _arr.push(block)
    if _arr.size() < 4 then
      _br.pull()
    else
      _br.close()
      _runTest()
    end

  fun _runTest() =>
    _t.complete_action("generated")
    _t.complete(true)

    _
