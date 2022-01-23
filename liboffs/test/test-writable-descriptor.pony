use "ponytest"
use "../BlockCache"
use "../OFFStreams"
use "Streams"
use "../Global"
use "files"
use "Base58"
use "Config"
use "Exception"
use "Buffer"

class iso _TestWriteableDescriptor is UnitTest
  fun name(): String => "Testing Writeable Descriptor"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(20000000000)
    t.expect_action("generated")
    t.expect_action("descriptor")
    let tests: _WriteableDescriptorTester[Mini] = _WriteableDescriptorTester[Mini](t)
    tests._start()




actor _WriteableDescriptorTester[B: BlockType]
  let _t: TestHelper
  var _br: (NewBlocksRecipe[B] | None) = None
  var _wd: (WriteableDescriptor[B] | None) = None
  var _size: USize = 0
  var _tuple: Array[Buffer val] iso
  var _tupleSize: USize = 0
  var _i: USize = 0
  new create(t: TestHelper) =>
    _t = t
    _tuple = recover Array[Buffer val](3) end
    try
      let path: FilePath = FilePath(t.env.root, "offs/blocks/")
      let conf: Config val = DefaultConfig()
      let descriptorPad = conf("descriptorPad")? as USize
      let tupleSize = conf("tupleSize")? as USize
      let blockSize = BlockSize[B]()
      _size = blockSize / descriptorPad
      let bc: BlockCache[B] = NewBlockCache[B](conf, path)?
      let br: NewBlocksRecipe[B] = NewBlocksRecipe[B](bc)
      let dataLength = (_size + 20) * blockSize
      let wd = WriteableDescriptor[B](bc, descriptorPad, tupleSize, dataLength)
      _wd = wd
      _br = br
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _WriteableDescriptorTester[B] = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end
      let readableNotify: ReadableNotify iso = object iso is ReadableNotify
        let test:  _WriteableDescriptorTester[B] = this
        fun apply() =>
          test._start()
      end
      let dataNotify: DataNotify[Block[B]] iso = object iso is DataNotify[Block[B]]
        let test: _WriteableDescriptorTester[B] = this
        fun apply(block: Block[B]) =>
          test._receiveBlock(block)
      end
      let closeNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _WriteableDescriptorTester[B] = this
        fun apply() =>
          _t.log("Block Recipe Closed")
      end
      let descriptorNotify: DescriptorNotify iso = object iso is DescriptorNotify
        let t: TestHelper = _t
        let test: _WriteableDescriptorTester[B] = this
        fun apply(descriptorHash: Buffer val) =>
          _t.complete_action("descriptor")
          try
            _t.log("Descriptor Received " + Base58.encode(descriptorHash.data)?)
          end

      end
      let descriptorCloseNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _WriteableDescriptorTester[B] = this
        fun apply() =>
          _t.log("Descriptor Closed")
          test._runTest()
      end
      let descriptorErrorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _WriteableDescriptorTester[B] = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end
      wd.subscribe(consume descriptorCloseNotify)
      wd.subscribe(consume descriptorNotify)
      wd.subscribe(consume descriptorErrorNotify)
      br.subscribe(consume errorNotify)
      br.subscribe(consume dataNotify)
      br.subscribe(consume readableNotify)
      br.subscribe(consume closeNotify)
    else
      t.fail("Block Cache Creation Error")
      t.complete(true)
    end

  be _start() =>
    match _br
      | let br: NewBlocksRecipe[B] =>
        br.pull()
    end

  be _receiveBlock(block: Block[B] val) =>
    _tuple.push(block.hash)
    _i = _i + 1
    match _wd
    | let wd: WriteableDescriptor[B]  =>
      if (_tuple.size() == 3) then
        wd.write(_tuple = recover Array[Buffer val](3) end)
      end
      match _br
      | let br: NewBlocksRecipe[B] =>
          if _i < _size then
              br.pull()
          else
            br.close()
            wd.close()
            _runTest()
          end
      end
    end

  be _runTest() =>
    _t.complete_action("generated")
    _t.complete(true)
