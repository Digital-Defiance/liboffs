use "pony_test"
use "../BlockCache"
use "../OFFStreams"
use "Streams"
use "../Global"
use "files"
use "Config"
use "Exception"
use "Buffer"
use "Blake3"

class iso _TestOffStream is UnitTest
  fun name(): String => "Testing Off Streams"
  fun exclusion_group(): String => "Block Cache"
  fun ref set_up(t: TestHelper) =>
    try
      let offDir = Directory(FilePath(FileAuth.create(t.env.root), "offs/"))?
      offDir.remove("blocks")
    end
  fun apply(t: TestHelper) =>
    t.long_test(20000000000)
    t.expect_action("filehash")
    t.expect_action("descriptorhash")
    t.expect_action("ORI")
    t.expect_action("file")
    t.expect_action("verified")
    let conf: Config val = DefaultConfig()

    try
      let descriptorPad = conf("descriptorPad")? as USize
      let tupleSize = conf("tupleSize")? as USize
      let tc: TupleCache = TupleCache(500)
      let path: FilePath = FilePath(FileAuth.create(t.env.root), "offs/blocks/")
      let bc: BlockCache[Standard] = NewBlockCache[Standard](conf, path)?
      let testerR: _ReadableOffStreamTester[Standard] = _ReadableOffStreamTester[Standard](t, conf, tc, bc)
      let cb = {(ori: ORI iso) (tester: _ReadableOffStreamTester[Standard] = testerR) =>
        tester.receiveORI(consume ori)
      } val
      let testerW: _WriteableOffStreamTester[Standard] = _WriteableOffStreamTester[Standard](t, conf, tc, bc, cb)
    else
      t.fail("Config Invalid")
      t.complete(true)
    end



actor _ReadableOffStreamTester[B: BlockType]
    let _t: TestHelper
    var _ori: (ORI val | None) = None
    var _rd: (ReadableDescriptor[B] | None) = None
    var _ws: (WriteableFileStream | None) = None
    var _rs: (ReadableOffStream[B] | None) = None
    let _bc: BlockCache[B]
    let _tc: TupleCache
    var _descriptorPad: USize = 0

  new create(t: TestHelper, config: Config val, tc: TupleCache, bc: BlockCache[B]) =>
    _t = t
    _bc = bc
    _tc = tc
    try
      _descriptorPad = config("descriptorPad")? as USize
      let tupleSize = config("tupleSize")? as USize
      let filePath = FilePath(FileAuth.create(t.env.root), "test.pdf")
      let file: File iso = recover File(filePath) end
      _ws = WriteableFileStream(consume file)
    else
      _t.fail("Failed to Configure Readable OFF Strean Test")
      _t.complete(true)
    end
  be complete() =>
    _t.complete_action("file")
    let fileSrcPath = FilePath(FileAuth.create(_t.env.root), "liboffs/test/test.pdf")
    let fileDstPath = FilePath(FileAuth.create(_t.env.root), "test.pdf")

    let fileSrc = File(fileSrcPath)
    let fileDst = File(fileDstPath)
    _t.assert_true(fileSrc.size() == fileDst.size())


    match _ori
    | let ori: ORI val =>
        let hasher = Blake3(_descriptorPad)
        hasher.update(fileDst.read(fileDst.size()))

        let fileDstHash = Buffer.fromArray(hasher.digest())
        _t.assert_true(fileDstHash == ori.fileHash)
        _t.complete_action("verified")
    end
    _t.complete(true)


  be receiveORI(ori: ORI iso) =>
    _t.complete_action("ORI")
    match _ws
      | let ws: WriteableFileStream =>
        let ori': ORI val = consume ori
        _ori = ori'
        let rs: ReadableOffStream[B] = ReadableOffStream[B](_bc,_tc, ori', _descriptorPad)
        let rd: ReadableDescriptor[B] = ReadableDescriptor[B](_bc, ori', _descriptorPad)
        let errorRDNotify: ErrorNotify iso = object iso is ErrorNotify
          let _t: TestHelper = _t
          fun ref apply(ex: Exception) =>
            _t.fail(ex.string())
            _t.complete(true)
        end
        rd.subscribe(consume errorRDNotify)
        let errorRSNotify: ErrorNotify iso = object iso is ErrorNotify
          let _t: TestHelper = _t
          fun ref apply(ex: Exception) =>
            _t.fail(ex.string())
            _t.complete(true)
        end
        rs.subscribe(consume errorRSNotify)
        let errorWSNotify: ErrorNotify iso = object iso is ErrorNotify
          let _t: TestHelper = _t
          fun ref apply(ex: Exception) =>
            _t.fail(ex.string())
            _t.complete(true)
        end
        ws.subscribe(consume errorWSNotify)
        let closeWSNotify: CloseNotify iso = object iso is CloseNotify
          let _tester: _ReadableOffStreamTester[B] =  this
          fun ref apply() =>
            _tester.complete()
        end
        ws.subscribe(consume closeWSNotify)
        rs.pipe(ws)
        rd.pipe(rs)
    end


actor _WriteableOffStreamTester[B: BlockType]
  let _t: TestHelper
  var _wd: (WriteableDescriptor[B] | None) = None
  var _ws: (WriteableOffStream[B] | None) = None
  var _rs: (ReadableFileStream | None) = None
  var _ori: (ORI | None) = None
  var _onDescriptorHash: {(ORI iso)} val
  new create(t: TestHelper, config: Config val, tc: TupleCache, bc: BlockCache[B], onDescriptorHash: {(ORI iso)} val) =>
    _t = t
    _onDescriptorHash = onDescriptorHash
    try
      let descriptorPad = config("descriptorPad")? as USize
      let tupleSize = config("tupleSize")? as USize
      let blockSize = BlockSize[B]()
      let nbr: NewBlocksRecipe[B] = NewBlocksRecipe[B](bc)
      let rpbr: RandomPopularityRecipe[B] = RandomPopularityRecipe[B](bc)
      let recipes: Array[BlockRecipe[B] tag] iso = recover Array[BlockRecipe[B] tag] end
      recipes.push(rpbr)
      recipes.push(nbr)
      let filePath = FilePath(FileAuth.create(t.env.root), "liboffs/test/test.pdf")
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

  fun ref _buildORI() =>
    match _ori
      | let ori: ORI =>
        if (ori.fileHash.size() > 0) and (ori.descriptorHash.size() > 0) then
          let rORI = recover ORI(where finalByte' = ori.finalByte, fileHash' = ori.fileHash, fileName' = ori.fileName, tupleSize' = ori.tupleSize, descriptorHash' = ori.descriptorHash) end
          _onDescriptorHash(consume rORI)
        end
    end

  be receiveFileHash (fileHash: Buffer val) =>
    match _ori
      | let ori: ORI =>
         ori.fileHash = fileHash
    end
    _buildORI()
    _t.complete_action("filehash")

  be receiveDescriptorHash(descriptorHash: Buffer val) =>
    match _ori
      | let ori: ORI =>
         ori.descriptorHash = descriptorHash
    end
    _buildORI()
    _t.complete_action("descriptorhash")

  be complete() =>
    None
