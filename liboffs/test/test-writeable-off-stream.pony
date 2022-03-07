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

class iso _TestWriteableOffStream is UnitTest
  fun name(): String => "Testing Writeable Off Stream"
  fun exclusion_group(): String => "Block Cache"
  fun ref set_up(t: TestHelper) =>
    try
      let offDir = Directory(FilePath(t.env.root, "offs/"))?
      offDir.remove("blocks")
    end
  fun apply(t: TestHelper) =>
    t.long_test(20000000000)
    t.expect_action("filehash")
    t.expect_action("descriptorhash")
    let tests: _WriteableOffStreamTester[Standard] = _WriteableOffStreamTester[Standard](t)


actor _WriteableOffStreamTester[B: BlockType]
  let _t: TestHelper
  var _wd: (WriteableDescriptor[B] | None) = None
  var _ws: (WriteableOffStream[B] | None) = None
  var _rs: (ReadableFileStream | None) = None
  var _ori: (ORI | None) = None
  new create(t: TestHelper) =>
    _t = t
    try
      let path: FilePath = FilePath(t.env.root, "offs/blocks/")
      let conf: Config val = DefaultConfig()
      let descriptorPad = conf("descriptorPad")? as USize
      let tupleSize = conf("tupleSize")? as USize
      let blockSize = BlockSize[B]()
      let bc: BlockCache[B] = NewBlockCache[B](conf, path)?
      let nbr: NewBlocksRecipe[B] = NewBlocksRecipe[B](bc)
      let rpbr: RandomPopularityRecipe[B] = RandomPopularityRecipe[B](bc)
      let recipes: Array[BlockRecipe[B] tag] iso = recover Array[BlockRecipe[B] tag] end
      recipes.push(rpbr)
      recipes.push(nbr)
      let tc: TupleCache = TupleCache(250)
      let filePath = FilePath(t.env.root, "liboffs/test/test.pdf")
      let file: File iso =recover File(filePath) end
      let ori: ORI = ORI(where finalByte' = file.size())
      ori.tupleSize = tupleSize
      ori.fileName = "test.pdf"
      _ori = ori
      let wd = WriteableDescriptor[B](bc, descriptorPad, tupleSize, file.size())
      let rs = ReadableFileStream(consume file)
      let ws = WriteableOffStream[B](bc, tc, consume recipes, descriptorPad, tupleSize)

      let errorWDNotify: ErrorNotify iso = object iso is ErrorNotify
        let _t: TestHelper = t
        fun ref apply(ex: Exception) =>
          _t.fail(ex.string())
          _t.complete(true)
      end
      wd.subscribe(consume errorWDNotify)
      let errorRSNotify: ErrorNotify iso = object iso is ErrorNotify
        let _t: TestHelper = t
        fun ref apply(ex: Exception) =>
          _t.fail(ex.string())
          _t.complete(true)
      end
      rs.subscribe(consume errorRSNotify)
      let errorWSNotify: ErrorNotify iso = object iso is ErrorNotify
        let _t: TestHelper = t
        fun ref apply(ex: Exception) =>
          _t.fail(ex.string())
          _t.complete(true)
      end
      ws.subscribe(consume errorWSNotify)

      let fileHashNotify: FileHashNotify iso = object iso is FileHashNotify
        let _tester: _WriteableOffStreamTester[B] =  this
        fun ref apply(fileHash: Buffer val) =>
          _tester.receiveFileHash(fileHash)
      end
      ws.subscribe(consume fileHashNotify)
      let descriptorHashNotify: DescriptorHashNotify iso = object iso is DescriptorHashNotify
        let _tester: _WriteableOffStreamTester[B] =  this
        fun ref apply(descriptorHash: Buffer val) =>
          _tester.receiveDescriptorHash(descriptorHash)
      end
      wd.subscribe(consume descriptorHashNotify)
      let closeWDNotify: CloseNotify iso = object iso is CloseNotify
        let _tester: _WriteableOffStreamTester[B] =  this
        fun ref apply() =>
          _tester.complete()
      end
      wd.subscribe(consume closeWDNotify)
      ws.pipe(wd)
      rs.pipe(ws)

      _wd = wd
      _rs = rs
      _ws = ws
    else
      _t.fail("Failed to construct dependencies")
      _t.complete(true)
    end

  be receiveFileHash (fileHash: Buffer val) =>
    //_ori.fileHash = fileHash
    _t.complete_action("filehash")
  be receiveDescriptorHash(descriptorHash: Buffer val) =>
    //_ori.descriptorHash = descriptorHash
    _t.complete_action("descriptorhash")

  be complete() =>
    _t.complete(true)
