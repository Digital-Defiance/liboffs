use "pony_test"
use "collections"
use "files"
use "../BlockCache"
use "../Global"
use "Exception"

actor BlockCacheTester[B: BlockType]
  let _t: TestHelper
  let _blocks: Array[Block[B]] val
  let _cb: {()} val
  var _path: (FilePath | None) = None
  var _i: USize = 0
  let _bc: BlockCache[B]
  var putTestComplete: Bool = false
  var getTestComplete: Bool = false
  var removeTestComplete: Bool = false
  var rankTestComplete: Bool = false
  new create(t: TestHelper, blocks: Array[Block[B]] val, path': FilePath, bc': BlockCache[B], cb: {()} val) =>
    _blocks = blocks
    _t = t
    _cb = cb
    _bc = bc'

  be apply() =>
    if putTestComplete == false then
      testPut()
    elseif getTestComplete == false then
      testGet()
    elseif rankTestComplete == false then
      testRanks()
    elseif removeTestComplete == false then
      testRemove()
    else
      _cb()
    end
  be _receiveRanks(ranks': Array[U64] iso) =>
    let ranks: Array[U64] box = consume ranks'
    for i in ranks.values() do
      _t.log(i.string())
    end
    rankTestComplete = true
    apply()
  be testRanks() =>
    _bc.ranks({(ranks: Array[U64] iso) (that: BlockCacheTester[B] = this) => that._receiveRanks(consume ranks)})

  be testPut() =>
    if _i < _blocks.size() then
      try
        _bc.put(_blocks(_i = _i + 1)?, {(err: (None | Exception)) (bc: BlockCacheTester[B] = this) =>
          match err
            | None =>
              bc.testPut()
            | let err': Exception =>
              _t.fail(err'.string())
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
        _bc.get(_blocks(_i = _i + 1)?.hash, {(block: (Block[B] | Exception | BlockNotFound)) (bc: BlockCacheTester[B] = this, i = (_i - 1)) =>
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
          _bc.remove(_blocks(_i = _i + 1)?.hash, {(err: (None | Exception | BlockNotFound)) (bc: BlockCacheTester[B] = this, i = (_i - 1), _t) =>
            match err
              | None =>
                try
                    _bc.get(_blocks(i)?.hash, {(block: (Block[B] | Exception | BlockNotFound)) (bc,_t) =>
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
  fun ref set_up(t: TestHelper) =>
    try
      let offDir = Directory(FilePath(FileAuth.create(t.env.root), "offs/"))?
      offDir.remove("blocks")
    end
  fun apply(t: TestHelper) =>
    t.long_test(5000000000)
    try
      let blocks: Array[Block[Standard]] val = recover
        let bs: BlockService[Standard] = BlockService[Standard]
        let blocks': Array[Block[Standard]] = Array[Block[Standard]](20)
          for i in Range(0, 20) do
            blocks'.push(bs.newBlock()?)
          end
          blocks'
      end
      let path: FilePath = FilePath(FileAuth.create(t.env.root), "offs/blocks/")
      let bc': BlockCache[Standard] = NewBlockCache[Standard](DefaultConfig(), path)?
      let bc = BlockCacheTester[Standard](t, blocks, path, bc', {() =>
        t.complete(true)
      } val)
      bc()
    else
      t.fail("Creation Error")
      t.complete(true)
    end
