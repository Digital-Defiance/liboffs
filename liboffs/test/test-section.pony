use "ponytest"
use "../BlockCache"
use "collections"
use "files"

interface WriteNextLoop
  be loop(index: (USize | SectionWriteError))
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
      let cb = {() (t) =>
        t.complete(true)
      } val
      let next = object is WriteNextLoop
        var _blocks: List[Block[Nano]] val = blocks
        var _cb : {()} val = cb
        var _i: USize = 0
        var _section: Section[Nano] = section
        var _t: TestHelper = t
        var blockIndex: Index iso = recover Index(5) end
        be apply() =>
          if _i < _blocks.size() then
            try
              _section.write(_blocks(_i = _i + 1)?, {(index: (USize | SectionWriteError)) (next : WriteNextLoop tag = this) => next.loop(index) })
            else
              _t.fail("block error")
              _t.complete(true)
            end
          else
            _cb()
          end
        be loop(index: (USize | SectionWriteError)) =>
          match index
            | SectionWriteError =>
              _t.fail("SectionWriteError")
              _t.complete(true)
            | let index': USize =>
              try
                //let indexEntry: IndexEntry iso = recover IndexEntry(_blocks(_i - 1)?.key, 1, index') end
                blockIndex.add(recover iso IndexEntry(_blocks(_i - 1)?.key, 1, index') end)?
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
                _cb()
              end
          end
      end
      next()
    else
      t.fail("Index Failed")
    end
