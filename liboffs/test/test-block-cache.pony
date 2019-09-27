use "ponytest"
use "collections"
use "files"
use "../BlockCache"

actor BlockCacheTester[B: BlockType]
  let _t: TestHelper
  let _blocks: List[Block[B]] val
  let _cb: {()} val
  var _path: (FilePath | None) = None
  var _i: USize = 0
  let _bc: BlockCache[B]
  var putTestComplete: Bool = false
  var getTestComplete: Bool = false
  var removeTestComplete: Bool = false

  new create(t: TestHelper, blocks: List[Block[B]] val, path': FilePath, cb: {()} val) =>
    _blocks = blocks
    _t = t
    _cb = cb
    _bc = BlockCache[B](path')

  be apply() =>
    if putTestComplete == false then
      testPut()
    elseif getTestComplete == false then
      testGet()
    elseif removeTestComplete == false then
      testRemove()
    else
      _cb()
    end
  be testPut() =>
    if _i < _blocks.size() then
      try
        _bc.put(_blocks(_i = _i + 1)?, {(err: (None | SectionWriteError | InvalidCacheError )) (bc: BlockCacheTester[B] = this) =>
          match err
            | None =>
              bc.testPut()
            | SectionWriteError =>
              _t.fail("Section Write Error")
              _t.complete(true)
            | InvalidCacheError =>
              _t.fail("Invalid Cache Error")
              _t.complete(true)
          end
        })
      else
        _t.fail("block error")
        _t.complete(true)
      end
    else
      putTestComplete = true
      _i = 0
      apply()
    end
  be testGet() =>
    if _i < _blocks.size() then
      try
        _bc.get(_blocks(_i = _i + 1)?.hash, {(block: (Block[B] | SectionReadError | InvalidCacheError | BlockNotFound)) (bc: BlockCacheTester[B] = this, i = (_i - 1)) =>
          match block
            | let block': Block[B] =>
              try
                _t.assert_array_eq[U8](block'.data, _blocks(i)?.data)
                _t.assert_array_eq[U8](block'.hash, _blocks(i)?.hash)
                bc.testGet()
              else
                _t.fail("Block Index not found")
                _t.complete(true)
              end
            else
              _t.fail("Retrieval Error")
              _t.complete(true)
          end
        })
      else
        _t.fail("block error")
        _t.complete(true)
      end
    else
      getTestComplete = true
      _i = 0
      apply()
    end
    be testRemove() =>
      if _i < _blocks.size() then
        try
          _bc.remove(_blocks(_i = _i + 1)?.hash, {(err: (None | SectionDeallocateError | InvalidCacheError | BlockNotFound)) (bc: BlockCacheTester[B] = this, i = (_i - 1), _t) =>
            match err
              | None =>
                try
                  _bc.get(_blocks(i)?.hash, {(block: (Block[B] | SectionReadError | InvalidCacheError | BlockNotFound)) (bc,_t) =>
                    match block
                      | let block': Block[B] =>
                        _t.fail("Block found after Removal")
                        _t.complete(true)
                    else
                      bc.testRemove()
                    end
                  })
                else
                  _t.fail("Block Index not found")
                  _t.complete(true)
                end
              else
                _t.fail("Removal Error")
                _t.complete(true)
            end
          })
        else
          _t.fail("block error")
          _t.complete(true)
        end
      else
        removeTestComplete = true
        _i = 0
        apply()
      end
class iso _TestBlockCache is UnitTest
  fun name(): String => "Testing Block Cache"
  fun exclusion_group(): String => "Block Cache"
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    try
      let blocks: List[Block[Standard]] val = recover
        let blocks': List[Block[Standard]] = List[Block[Standard]](20)
          for i in Range(0, 20) do
            blocks'.push(Block[Standard]()?)
          end
          blocks'
      end
      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/blocks/")?
      let offDir = Directory(FilePath(t.env.root as AmbientAuth, "offs/")?)?
      offDir.remove("blocks")
      let bc = BlockCacheTester[Standard](t, blocks, path, {() =>
        t.complete(true)
      } val)
      bc()
    else
      t.fail("Creation Error")
      t.complete(true)
    end
