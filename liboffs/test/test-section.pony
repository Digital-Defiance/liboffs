use "ponytest"
use "../BlockCache"
use "collections"
use "files"

interface WriteNextLoop
  be loop(index: ((USize, Bool) | SectionWriteError))
  be apply()

interface ReadNextLoop
  be loop(index: (Array[U8] val | SectionReadError))
  be apply()

interface DeallocateNextLoop
  be loop(ok: (None | SectionDeallocateError))
  be apply()

class iso _TestSection is UnitTest
  fun name(): String => "Testing Section"
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


      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/nano/sections/data/")?
      let metaPath: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/nano/sections/meta/")?
      path.mkdir()
      metaPath.mkdir()

      let metaDir = Directory(metaPath)?
      metaDir.remove("4000")

      let section: Section[Nano] = Section[Nano](path, metaPath, 20, 4000)
      let cb = {(index: Index iso) (t, section) =>
        let indexes : Array[USize] val = recover
          let indexes' : Array[USize] = Array[USize](blocks.size())
          let index': Index = consume index
          try
            for block in blocks.values() do
              match index'.find(block.hash)?
                | None =>
                  t.fail("Index error")
                  t.complete(true)
                | let indexEntry : IndexEntry =>
                  indexes'.push(indexEntry.sectionIndex)
              end
            end
          else
            t.fail("Index error")
            t.complete(true)
          end
           indexes'
        end

        let cb = {() (t, section, indexes) =>
          let cb = {() (t, section, indexes) =>
            let cb = {() (t) =>
              t.complete(true)
            } val

            let newBlocks: List[Block[Nano]] val = recover
              let newBlocks': List[Block[Nano]] = List[Block[Nano]](4)
              for i in Range(2,6) do
                try
                  newBlocks'.push(Block[Nano]()?)
                else
                  t.fail("Block Creation Error")
                  t.complete(true)
                end
              end
              newBlocks'
            end

            let next = object is WriteNextLoop
              var _blocks: List[Block[Nano]] val = newBlocks
              var _cb : {()} val = cb
              var _i: USize = 0
              var _newId: USize = 2
              var _section: Section[Nano] = section
              var _t: TestHelper = t
              var _indexes : Array[USize] val = indexes

              be apply() =>
                if _i < _blocks.size() then
                  try
                    _section.write(_blocks(_i = _i + 1)?, {(index: ((USize, Bool) | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
                  else
                    _t.fail("block error")
                    _t.complete(true)
                  end
                else
                  _cb()
                end
              be loop(index: ((USize, Bool) | SectionWriteError)) =>
                match index
                  | SectionWriteError =>
                    _t.fail("SectionWriteError" + _i.string())
                    _t.complete(true)
                  | (let index': USize,  let full: Bool) =>
                    try
                      _t.assert_true(index' == _indexes(_newId = _newId + 1)?)
                    else
                      _t.fail("block index error")
                      _t.complete(true)
                    end
                    if _i < _blocks.size() then
                      try
                        _section.write(_blocks(_i = _i + 1)?, {(index: ((USize, Bool) | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
                      else
                        _t.fail("block error")
                        _t.complete(true)
                      end
                    else
                      _cb()
                    end
                end
            end
            next()
          } val

          let next = object is DeallocateNextLoop
            var _i: USize = 2
            let _indexes: Array[USize] val = indexes
            let _cb: {()} val = cb
            let _t: TestHelper = t
            be apply() =>
              if _i < 6 then
                try
                  section.deallocate(_indexes(_i = _i + 1)?, {(ok: (None | SectionDeallocateError)) (next : DeallocateNextLoop tag = this) => next.loop(ok) } val)
                else
                  _t.fail("Index Error")
                  _t.complete(true)
                end
              else
                _cb()
              end
            be loop(ok: (None | SectionDeallocateError)) =>
              match ok
                | SectionDeallocateError =>
                  _t.fail("SectionDeallocateError")
                  _t.complete(true)
              end
              if _i < 6 then
                try
                  section.deallocate(_indexes(_i = _i + 1)?, {(ok: (None | SectionDeallocateError)) (next : DeallocateNextLoop tag = this) => next.loop(ok) } val)
                else
                  _t.fail("Index Error")
                  _t.complete(true)
                end
              else
                _cb()
              end
          end
          next()
        } val
        let next = object is ReadNextLoop
          var _blocks: List[Block[Nano]] val = blocks
          var _cb : {()} val = cb
          var _i: USize = 0
          var _section: Section[Nano] = section
          var _t: TestHelper = t
          var _indexes: Array[USize] val = indexes

          be apply() =>
            if _i < _indexes.size() then
              try
                _section.read(_indexes(_i = _i + 1)?, {(data: (Array[U8] val | SectionReadError)) (next : ReadNextLoop tag = this) => next.loop(data) })
              else
                _t.fail("block error")
                _t.complete(true)
              end
            else
              _cb()
            end

          be loop(data: (Array[U8] val | SectionReadError)) =>
            match data
              | SectionReadError =>
                _t.fail("SectionReadError")
                _t.complete(true)
              | let data' : Array[U8] val =>
                try
                  if _i < _indexes.size() then
                    let block: Block[Nano] = Block[Nano](data')?
                    t.assert_array_eq[U8](block.data, _blocks(_i - 1)?.data)
                    _section.read(_indexes(_i = _i + 1)?, {(data: (Array[U8] val | SectionReadError)) (next : ReadNextLoop tag = this) => next.loop(data) })
                  else
                    _cb()
                  end
                else
                  _t.fail("Block Error")
                  _t.complete(true)
                end
            end
        end
        next()
      } val
      let next = object is WriteNextLoop
        var _blocks: List[Block[Nano]] val = blocks
        var _cb : {(Index iso)} val = cb
        var _i: USize = 0
        var _section: Section[Nano] = section
        var _t: TestHelper = t
        var _blockIndex: Index iso = recover Index(5, FilePath(t.env.root as AmbientAuth, "offs/blocks/nano/index/")?)? end
        be apply() =>
          try
            if _i < _blocks.size() then
              _section.write(_blocks(_i = _i + 1)?, {(index: ((USize, Bool) | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
            else
              let index'' : Index iso = _blockIndex = recover Index(5, FilePath(t.env.root as AmbientAuth, "offs/blocks/nano/index/")?)? end
              _cb(consume index'')
            end
          else
            _t.fail("block error")
            _t.complete(true)
          end
        be loop(index: ((USize, Bool) | SectionWriteError)) =>
          match index
            | SectionWriteError =>
              _t.fail("SectionWriteError this one")
              _t.complete(true)
            | (let index': USize, let full: Bool) =>
              try
                _blockIndex.add(recover iso IndexEntry(_blocks(_i - 1)?.hash, 1, index') end)?
              else
                _t.fail("block index error")
                _t.complete(true)
              end
              try
                if _i < _blocks.size() then
                  _section.write(_blocks(_i = _i + 1)?, {(index: ((USize, Bool) | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
                else
                  let index'' : Index iso = _blockIndex = recover Index(5, FilePath(t.env.root as AmbientAuth,  "offs/blocks/nano/sections/")?)? end
                  _cb(consume index'')
                end
              else
                _t.fail("block error")
                _t.complete(true)
              end
          end
      end
      next()
    else
      t.fail("Index Failed")
    end
