use "files"
use "time"
use "LRUCache"
use "collections"
use "Config"
use "Buffer"

primitive InvalidCacheError
primitive BlockNotFound

actor BlockCache [B: BlockType]
  var _path: (FilePath | None) = None
  let _timers: Timers =  Timers
  var _curTimer: (Timer iso! | None) = None
  var _index: (Index | None) = None
  var _sections: (Sections[B] | None) = None
  let _entryCheckout: MapIs[Buffer val, IndexEntry] = MapIs[Buffer val, IndexEntry]
  var _config: Config val
  let _blocks: LRUCache[Buffer val, Block[B] val]

  new create(config': Config val, path': FilePath) =>
    _config = config'
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

    _blocks = try LRUCache[Buffer val, Block[B] val](_config("lruSize")? as USize) else LRUCache[Buffer val, Block[B] val](20) end
    _path = try FilePath(path', name)? else None end
    match _path
      | let path :FilePath =>
        path.mkdir()
        _index = try Index((_config("indexNodeSize")? as USize), path)? else None end
        _sections = try Sections[B](path, (_config("sectionSize")? as USize), (_config("maxTupleSize")? as USize)) else None end// TODO: How large should a section be?
    end

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
      match _index
        | let index: Index =>
          index.remove(hash)?
      end
    else
      None
    end
  be _cacheBlock(block: Block[B] val) =>
    _blocks(block.hash) = block

  be put(block: Block[B], cb: {((None | SectionWriteError | InvalidCacheError ))} val) =>
      match _index
        | None =>
          cb(InvalidCacheError)
          return
        | let index: Index =>
          try
            let entry: IndexEntry =  index.get(block.hash)?
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
              match _sections
                | let sections: Sections[B] =>
                  sections.write(block, cb')
              else
                cb(InvalidCacheError)
                return
              end
              _save()
            else
              cb(None)
            end
          else
            cb(SectionWriteError)
          end
      end

  be get(hash: Buffer val, cb: {((Block[B] | SectionReadError | InvalidCacheError | BlockNotFound))} val) =>
    match _blocks(hash)
      | let block: Block[B] =>
        cb(block)
      | None =>
        match _index
          | None =>
            cb(InvalidCacheError)
            return
          | let index: Index =>
            try
              match index.find(hash)?
                | let entry: IndexEntry =>
                  let cb' = {(data: (Buffer val | SectionReadError)) (blockCache: BlockCache[B] = this) =>
                    match data
                      | SectionReadError =>
                        cb(SectionReadError)
                      | let data': Buffer val =>
                        try
                          let block: Block[B] = Block[B]._withHash(data', hash)?
                          blockCache._cacheBlock(block)
                          cb(block)
                        else
                          cb(SectionReadError)
                        end
                    end
                  } val
                  match _sections
                    | let sections: Sections[B] =>
                      sections.read(entry.sectionId, entry.sectionIndex, cb')
                  else
                    cb(InvalidCacheError)
                    return
                  end
                  _save()
                | None =>
                  cb(BlockNotFound)
                  return
              end
            else
              cb(SectionReadError)
            end
        end
      end


  be remove(hash: Buffer val, cb: {((None | SectionDeallocateError | InvalidCacheError | BlockNotFound))} val) =>
    match _index
      | None =>
        cb(InvalidCacheError)
        return
      | let index: Index =>
        try
          match index.find(hash)?
            | let entry: IndexEntry =>
              let cb' = {(err: (None | SectionDeallocateError)) =>
                match err
                  | SectionDeallocateError =>
                    cb(SectionDeallocateError)
                  | None  =>
                    cb(None)
                end
              } val
              match _sections
                | let sections: Sections[B] =>
                  sections.deallocate(entry.sectionId, entry.sectionIndex, cb')
              end
              index.remove(hash)?
              _blocks.remove(hash)
              _save()
            | None =>
              cb(BlockNotFound)
              return
          end
        else
          cb(SectionDeallocateError)
        end
    end

  be _saveIndex() =>
    match _index
      | let index: Index =>
        index.save()
    end

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
