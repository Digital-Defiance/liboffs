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

class iso _TestReadableDescriptor is UnitTest
  fun name(): String => "Testing Readable Descriptor"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(6000000000000)
    t.expect_action("generated")
    t.expect_action("descriptor")
    t.expect_action("received")
    let tests: _ReadableDescriptorTester[Mini] = _ReadableDescriptorTester[Mini](t)
    tests._start()




actor _ReadableDescriptorTester[B: BlockType]
  let _t: TestHelper
  var _br: (NewBlocksRecipe[B] | None) = None
  var _wd: (WriteableDescriptor[B] | None) = None
  var _rd: (ReadableDescriptor[B] | None) = None
  var _size: USize = 0
  var _tuple: Tuple iso
  var _tupleSize: USize = 0
  var _i: USize = 0
  var _allBlockHashes: (Array[Buffer val] iso | None) = None
  var _bc: (BlockCache[B] | None) = None
  var _descriptorPad: (USize | None) = None
  var _dataLength: (USize | None) = None

  new create(t: TestHelper) =>
    _t = t
    _tuple = recover Tuple(3) end
    try
      let path: FilePath = FilePath(t.env.root, "offs/blocks/")
      let conf: Config val = DefaultConfig()
      _descriptorPad = conf("descriptorPad")? as USize
      let tupleSize = conf("tupleSize")? as USize
      _tupleSize = tupleSize
      let blockSize = BlockSize[B]()
      _size = 45
      _allBlockHashes = recover Array[Buffer val](_size) end
      _bc = NewBlockCache[B](conf, path)?
      let br: NewBlocksRecipe[B] = NewBlocksRecipe[B](_bc as BlockCache[B])
      _dataLength = _size * blockSize
      let wd = WriteableDescriptor[B](_bc as BlockCache[B], _descriptorPad as USize, tupleSize, _dataLength as USize)
      _wd = wd
      _br = br
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _ReadableDescriptorTester[B] = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end
      let readableNotify: ReadableNotify iso = object iso is ReadableNotify
        let test:  _ReadableDescriptorTester[B] = this
        fun apply() =>
          test._start()
      end
      let dataNotify: DataNotify[Block[B]] iso = object iso is DataNotify[Block[B]]
        let test: _ReadableDescriptorTester[B] = this
        fun apply(block: Block[B]) =>
          test._receiveBlock(block)
      end
      let closeNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _ReadableDescriptorTester[B] = this
        fun apply() =>
          _t.log("Block Recipe Closed")
      end
      let descriptorNotify: DescriptorNotify iso = object iso is DescriptorNotify
        let t: TestHelper = _t
        let test: _ReadableDescriptorTester[B] = this
        fun apply(descriptorHash: Buffer val) =>
          _t.complete_action("descriptor")
          test._runTest(descriptorHash)
          try
            _t.log("Descriptor Received " + Base58.encode(descriptorHash.data)?)
          end

      end
      let descriptorCloseNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _ReadableDescriptorTester[B] = this
        fun apply() =>
          _t.complete_action("generated")
          _t.log("Writeable Descriptor Closed")
      end
      let descriptorErrorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _ReadableDescriptorTester[B] = this
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
    try
      (_allBlockHashes as Array[Buffer val] iso).push(block.hash)
    else
      _t.fail("Failed to add block")
      _t.complete(true)
    end
    try
      _tuple.push(block.hash)?
    else
      _t.fail("Tuple Push Error")
      _t.complete(true)
    end
    _i = _i + 1
    match _wd
    | let wd: WriteableDescriptor[B]  =>
      if (_tuple.size() == 3) then
        wd.write(_tuple = recover Tuple(3) end)
      end
      match _br
      | let br: NewBlocksRecipe[B] =>
          if _i < _size then
              br.pull()
          else
            br.close()
            wd.close()
          end
      end
    end

  be _receiveTuple(tuple: Array[Buffer val] val) =>
    try
      for hash in tuple.values() do
        _t.assert_true((_allBlockHashes as Array[Buffer val] iso)((_i = _i + 1) % _tupleSize)? == hash)
      end
      if _i >= (_allBlockHashes as Array[Buffer val] iso).size() then
        _t.complete_action("received")
        _t.complete(true)
      end
    else
      _t.fail("Failed Tuple Check")
      _t.complete(true)
    end

  be _runTest(descriptorHash: Buffer val) =>
    try
      _i = 0
      let ori: ORI val = recover ORI(where descriptorHash' = descriptorHash, tupleSize' = _tupleSize, fileSize' = ((_dataLength as USize) / _tupleSize)) end
      let rd: ReadableDescriptor[B] = ReadableDescriptor[B](_bc as BlockCache[B], ori, _descriptorPad as USize)
      let dataNotify: DataNotify[Array[Buffer val] val] iso = object iso is DataNotify[Array[Buffer val] val]
        let test: _ReadableDescriptorTester[B] = this
        fun apply(tuple: Array[Buffer val] val) =>
          test._receiveTuple(tuple)
      end
      let closeNotify: CloseNotify iso = object iso is CloseNotify
        let t: TestHelper = _t
        let test: _ReadableDescriptorTester[B] = this
        fun apply() =>
          _t.log("Readable Descriptor Closed")
      end
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let test: _ReadableDescriptorTester[B] = this
        let t: TestHelper = _t
        fun apply(ex: Exception) =>
          t.fail(ex.string())
          t.complete(true)
      end

      rd.subscribe(consume closeNotify)
      rd.subscribe(consume errorNotify)
      rd.subscribe(consume dataNotify)
      _t.complete(true)
    else
      _t.fail("Failed to run test")
      _t.complete(true)
    end
