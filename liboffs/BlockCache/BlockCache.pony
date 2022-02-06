use "files"
use "time"
use "LRUCache"
use "collections"
use "Config"
use "Buffer"

primitive BlockNotFound

primitive NewBlockCache[B: BlockType]
  fun apply(config: Config val, path': FilePath ): BlockCache[B] ? =>
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
    let blocks: LRUCache[Buffer val, (Block[B] val, IndexEntry)] iso = recover LRUCache[Buffer val, (Block[B] val, IndexEntry)](config("lruSize")? as USize) end
    let path: FilePath = FilePath.from(path', name)?
    let index: Index iso = recover Index((config("indexNodeSize")? as USize), path)? end
    let sections: Sections[B] = Sections[B](path, (config("sectionSize")? as USize), (config("maxTupleSize")? as USize))// TODO: How large should a section be?
    BlockCache[B](config, consume blocks, consume index, sections)

actor BlockCache [B: BlockType]
  let _timers: Timers =  Timers
  var _curTimer: (Timer iso! | None) = None
  var _index: Index
  var _sections: Sections[B]
  let _entryCheckout: MapIs[Buffer val, IndexEntry] = MapIs[Buffer val, IndexEntry]
  var _config: Config val
  let _blocks: LRUCache[Buffer val, (Block[B] val, IndexEntry)]

  new create(config': Config val, blocks': LRUCache[Buffer val, (Block[B] val, IndexEntry)] iso, index': Index iso, sections': Sections[B]) =>
    _config = config'
    _blocks = consume blocks'
    _index = consume index'
    _sections = consume sections'

  be _checkIn(hash: Buffer val, sectionId: USize, sectionIndex: USize) =>
    try
      let entry: IndexEntry = _entryCheckout(hash)?
      entry.sectionId = sectionId
      entry.sectionIndex = sectionIndex
      _entryCheckout.remove(hash)?
    else
      None
    end

  be _removeIndex(hash: Buffer val) =>
    try
      _entryCheckout.remove(hash)?
      _index.remove(hash)?
    else
      None
    end

  be _cacheBlock(block: Block[B] val) =>
    try
      let entry: IndexEntry = _entryCheckout(block.hash)?
      _blocks(block.hash) = (block, entry)
    end

  be ranks(cb:{(Array[U64] iso)} val) =>
    let ranks': Map[U64, Array[IndexEntry]] box =_index.ranks()
    let rankKeys: Array[U64] iso = recover Array[U64](ranks'.size()) end
    for rank in ranks'.keys() do
      rankKeys.push(rank)
    end
    cb(consume rankKeys)

  be hashesAtRank(rank: U64, cb: {(Array[Buffer val] iso)} val) =>
    let ranks': Map[U64, Array[IndexEntry]] box =_index.ranks()
    try
      let entries: Array[IndexEntry] box = ranks'(rank)?
      let hashes: Array[Buffer val] iso = recover Array[Buffer val] end
      for entry in entries.values() do
        hashes.push(entry.hash)
      end
      cb(consume hashes)
    else
      cb(recover Array[Buffer val]end)
    end

  be put(block: Block[B], cb: {((None | SectionWriteError))} val) =>
    try
      let entry: IndexEntry =  _index.get(block.hash)?
      if entry.sectionId == 0 then
        if _entryCheckout.contains(block.hash) then
          cb(None)
          return
        end
        _entryCheckout(block.hash) = entry
        let cb' = {(id: ((USize, USize) | SectionWriteError)) (hash: Buffer val = block.hash, blockCache: BlockCache[B] = this) =>
          match id
            |  (let sectionId: USize, let sectionIndex: USize) =>
              blockCache._checkIn(hash, sectionId, sectionIndex)
              cb(None)
            | SectionWriteError =>
              blockCache._removeIndex(hash)
              cb(SectionWriteError)
          end
        } val
        _sections.write(block, cb')
        _save()
      else
        cb(None)
      end
    else
      cb(SectionWriteError)
    end

  be get(hash: Buffer val, cb: {((Block[B] | SectionReadError | BlockNotFound))} val) =>
    match _blocks(hash)
      | (let block: Block[B], let entry: IndexEntry ) =>
        _index._increment(entry)
        cb(block)
      | None =>
        try
          match _index.find(hash)?
            | let entry: IndexEntry =>
              _entryCheckout(hash) = entry
              let cb' = {(data: (Buffer val | SectionReadError)) (blockCache: BlockCache[B] = this, hash': Buffer val = entry.hash) =>
                match data
                  | SectionReadError =>
                    cb(SectionReadError)
                  | let data': Buffer val =>
                    try
                      let block: Block[B] = Block[B]._withHash(data', hash')?
                      blockCache._cacheBlock(block)
                      cb(block)
                    else
                      cb(SectionReadError)
                    end
                end
              } val
              _sections.read(entry.sectionId, entry.sectionIndex, cb')
              _save()
            | None =>
              cb(BlockNotFound)
              return
          end
        else
          cb(SectionReadError)
        end
      end

  be remove(hash: Buffer val, cb: {((None | SectionDeallocateError | BlockNotFound))} val) =>
    try
      match _index.find(hash)?
        | let entry: IndexEntry =>
          let cb' = {(err: (None | SectionDeallocateError)) =>
            match err
              | SectionDeallocateError =>
                cb(SectionDeallocateError)
              | None  =>
                cb(None)
            end
          } val
          _sections.deallocate(entry.sectionId, entry.sectionIndex, cb')
          _index.remove(hash)?
          _blocks.remove(hash)
          _save()
        | None =>
          cb(BlockNotFound)
          return
      end
    else
      cb(SectionDeallocateError)
    end

  be _saveIndex() =>
    _index.save()

  fun ref _save() =>
    match _curTimer
    | let curTimer': Timer iso! =>
        _timers.cancel(curTimer')
    end

    let saver: IndexSaver iso = IndexSaver({() (blockCache: BlockCache[B] = this) => blockCache._saveIndex()} val)
    let timer: Timer iso = Timer(consume saver, 500000, 500000)
    _curTimer = timer
    _timers(consume timer)




class IndexSaver is TimerNotify
  let _cb: {()} val
  new iso create(cb:{()} val) =>
    _cb = cb
  fun apply(timer: Timer, count: U64): Bool =>
    _cb()
    false
