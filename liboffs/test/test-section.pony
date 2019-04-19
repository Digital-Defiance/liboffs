use "ponytest"
use "../BlockCache"
use "collections"
use "files"

interface WriteNextLoop
  be loop(index: (USize | SectionWriteError))
  be apply()
interface ReadNextLoop
  be loop(index: (Array[U8] val | SectionReadError))
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


      let path: FilePath = FilePath(t.env.root as AmbientAuth, "section1")?
      let section: Section[Nano] = Section[Nano](path, 20, 1)
      let cb = {(index: Index iso) (t) =>
        let cb ={() (t) =>
          t.complete(true)
        } val
        let next = object is ReadNextLoop
          var _blocks: List[Block[Nano]] val = blocks
          var _cb : {(Index iso)} val = cb
          var _i: USize = 0
          var _section: Section[Nano] = section
          var _t: TestHelper = t
          var _blockIndex: Index iso = consume index

          be apply() =>
            if _i < _blocks.size() then
              try
                match _blockIndex.get(_blocks(_i = _i + 1)?.key)
                  | None =>
                    _t.fail("Index error")
                    _t.complete(true)
                  | let indexEntry: IndexEntry =>
                    _section.read(indexEntry.sectionIndex, {(data: (Array[U8] val | SectionReadError)) (next : WriteNextLoop tag = this) => next.loop(data) })
                end
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
                  let block: Block[Nano] = Block[Nano](data')
                  t.assert_array_eq[U8](block.data, _blockIndex(_blocks(_i - 1)?.data))
                  if _i < _blocks.size() then
                    try
                      match _blockIndex(_blocks(_i = _i + 1)?.key)
                        | None =>
                          _t.fail("Index error")
                          _t.complete(true)
                        | let indexEntry: IndexEntry =>
                          _section.read(indexEntry.sectionIndex, {(data: (Array[U8] val | SectionReadError)) (next : WriteNextLoop tag = this) => next.loop(data) })
                      end
                    else
                      _t.fail("block error")
                      _t.complete(true)
                    end
                  else
                    _cb()
                  end
                else
                  _t.fail("Block Error")
                  _t.complete(true)
                end
            end
        end
      } val
      let next = object is WriteNextLoop
        var _blocks: List[Block[Nano]] val = blocks
        var _cb : {(Index iso)} val = cb
        var _i: USize = 0
        var _section: Section[Nano] = section
        var _t: TestHelper = t
        var _blockIndex: Index iso = recover Index(5) end
        be apply() =>
          if _i < _blocks.size() then
            try
              _section.write(_blocks(_i = _i + 1)?, {(index: (USize | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
            else
              _t.fail("block error")
              _t.complete(true)
            end
          else
            let index'' : Index iso = _blockIndex = recover Index(5) end
            _cb(consume index'')
          end
        be loop(index: (USize | SectionWriteError)) =>
          match index
            | SectionWriteError =>
              _t.fail("SectionWriteError")
              _t.complete(true)
            | let index': USize =>
              try
                //let indexEntry: IndexEntry iso = recover IndexEntry(_blocks(_i - 1)?.key, 1, index') end
                _blockIndex.add(recover iso IndexEntry(_blocks(_i - 1)?.key, 1, index') end)?
              else
                _t.fail("block index error")
                _t.complete(true)
              end
              if _i < _blocks.size() then
                try
                  _section.write(_blocks(_i = _i + 1)?, {(index: (USize | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
                else
                  _t.fail("block error")
                  _t.complete(true)
                end
              else
                let index'' : Index iso = _blockIndex = recover Index(5) end
                _cb(consume index'')
              end
          end
      end
      next()
    else
      t.fail("Index Failed")
    end
