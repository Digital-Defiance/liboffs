use "ponytest"
use "../BlockCache"
use "collections"
use "files"

actor SectionsTester[B: BlockType]
  var _index: (Index | None) = None
  var _sections: (Sections[B] | None) =  None
  let _entryCheckout: MapIs[Array[U8] val, IndexEntry] = MapIs[Array[U8] val, IndexEntry]
  let _t: TestHelper
  let _blocks: List[Block[B]] val
  let _cb: {()} val
  var _path: (FilePath | None) = None
  var _i: USize = 0
  var _writeTestComplete: Bool = false
  var _readTestComplete: Bool = false

  new create(indexSize: USize, t: TestHelper, blocks: List[Block[B]] val, path': FilePath, cb: {()} val) =>
    _t = t
    _blocks = blocks
    let name: String = iftype B <: Mega then
      "mega/"
    elseif B <: Standard then
      "block/"
    elseif B <: Mini then
       "mini/"
    elseif B <: Nano then
      "nano/"
    else
      ""
    end

    _path = try FilePath(path', name)? else None end
    match _path
      | let path :FilePath => path.mkdir()
        _index = try Index(25, path)? else None end
        _sections = Sections[B](path, 3, _t)// TODO: How large should a section be?
    end
    _cb = cb

  be _checkIn(hash: Array[U8] val, sectionId: USize, sectionIndex: USize) =>
    try
      let entry: IndexEntry = _entryCheckout(hash)?
      entry.sectionId = sectionId
      entry.sectionIndex = sectionIndex
      _entryCheckout.remove(hash)?
    else
      _t.fail("check in error")
      _t.complete(true)
    end

  be _removeIndex(hash: Array[U8] val) =>
    try
      _entryCheckout.remove(hash)?
      match _index
        | let index: Index =>
          index.remove(hash)?
      end
    else
      _t.fail("block error")
      _t.complete(true)
    end

  be apply() =>
    if (_writeTestComplete != true) then
      if _i < _blocks.size() then
        try
          testWrite(_blocks(_i = _i + 1)?, {() (next: SectionsTester[B] = this) => next() })
        else
          _t.fail("block error")
          _t.complete(true)
        end
      else
        _writeTestComplete = true
        _i = 0
        apply()
      end
    elseif (_readTestComplete != true) then
      if _i < _blocks.size() then
        try
          testRead(_blocks(_i = _i + 1)?.hash, {( block: (Block[B] | SectionReadError)) (next: SectionsTester[B] = this, i = (_i - 1) ) =>
            match block
              | let block': Block[B] =>
                try
                  _t.assert_array_eq[U8](block'.data, _blocks(i)?.data)
                  _t.assert_array_eq[U8](block'.hash, _blocks(i)?.hash)
                else
                  _t.fail("Block Index not found")
                  _t.complete(true)
                end
              else
                _t.fail("Retrieval Error")
                _t.complete(true)
            end
            next()
          })
        else
          _t.fail("block error")
          _t.complete(true)
        end
      else
        _readTestComplete = true
        _i = 0
        _cb()
      end

    end

  be testWrite(block: Block[B] val, cb: {()} val) =>
    match _index
      | None =>
        _t.fail("Invalid Index")
        _t.complete(true)
        return
      | let index: Index =>
        try
          let entry: IndexEntry =  index.get(block.hash)?
          if entry.sectionId == 0 then
            if _entryCheckout.contains(block.hash) then
              cb()
              return
            end
            _entryCheckout(block.hash) = entry
            let cb' = {(id: ((USize, USize) | SectionWriteError)) (hash: Array[U8] val = block.hash, sectionsTester: SectionsTester[B] = this) =>
              match id
                |  (let sectionId: USize, let sectionIndex: USize) =>
                  sectionsTester._checkIn(hash, sectionId, sectionIndex)
                  cb()
                | SectionWriteError =>
                  sectionsTester._removeIndex(hash)
                  _t.fail("Section WriTestLoop Error")
                  _t.complete(true)
              end
            } val
            match _sections
              | let sections: Sections[B] =>
                sections.write(block, cb')
            else
              _t.fail("Invalid Sections")
              _t.complete(true)
              return
            end
          else
            cb()
          end
        else
          _t.fail("Section Write Error")
          _t.complete(true)
        end
    end

  be testRead(hash: Array[U8] val, cb: {((Block[B] | SectionReadError))} val) =>
    match _index
      | None =>
        _t.fail("Invalid Index")
        _t.complete(true)
        return
      | let index: Index =>
        try
          match index.find(hash)?
            | let entry: IndexEntry =>
              let cb' = {(data: (Array[U8] val | SectionReadError)) =>
                match data
                  | SectionReadError =>
                    _t.fail("Section Read Error")
                    _t.complete(true)
                  | let data': Array[U8] val =>
                    try
                      let block: Block[B] = Block[B](data')?
                      cb(block)
                    else
                      _t.fail("Block Creation Error")
                      _t.complete(true)
                    end
                end
              } val
              match _sections
                | let sections: Sections[B] =>
                  sections.read(entry.sectionId, entry.sectionIndex, cb')
              else
                _t.fail("Invalid Sections")
                _t.complete(true)
                return
              end
            | None =>
              _t.fail("Block Not Found")
              _t.complete(true)
              return
          end
        else
          cb(SectionReadError)
        end
    end

class iso _TestSections is UnitTest
  fun name(): String => "Testing Sections"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    try
      let blocks: List[Block[Nano]] val = recover
        let blocks': List[Block[Nano]] = List[Block[Nano]](20)
          for i in Range(0, 20) do
            blocks'.push(Block[Nano]()?)
          end
          blocks'
      end
      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/")?
      let offDir = Directory(FilePath(t.env.root as AmbientAuth, "offs/")?)?
      offDir.remove("blocks")
      let sectionTester = SectionsTester[Nano](4, t, blocks, path, {() =>
        t.complete(true)
      } val)
      sectionTester()
    else
      t.fail("Initial Failure")
      t.complete(true)
    end
