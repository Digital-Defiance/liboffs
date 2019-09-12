use "files"
use "time"
use "LRUCache"
use "collections"

primitive InvalidCacheError
primitive BlockNotFound

actor BlockCache [B: BlockType]
  var _path: (FilePath | None) = None
  let _timers: Timers =  Timers
  var _curTimer: (Timer iso! | None) = None
  var _index: (Index | None) = None
  var _sections: (Sections[B] | None) = None
  let _entryCheckout: MapIs[Array[U8] val, IndexEntry] = MapIs[Array[U8] val, IndexEntry]

  new create(path': FilePath) =>
    let name: String = iftype B <: Mega then
      ".mega/"
    elseif B <: Standard then
      ".block/"
    elseif B <: Mini then
       ".mini/"
    elseif B <: Nano then
      ".nano/"
    else
      ""
    end

    _path = try FilePath(path', name)? else None end
    match _path
      | let path :FilePath => path.mkdir()
        _index = try Index(25, path)? else None end
        _sections = Sections[B](path, 25)// TODO: How large should a section be?
    end
  be _checkIn(hash: Array[U8] val, sectionId: USize, sectionIndex: USize) =>
    try
      let entry: IndexEntry = _entryCheckout(hash)?
      entry.sectionId = sectionId
      entry.sectionIndex = sectionIndex
      _entryCheckout.remove(hash)?
    else
      None
    end
  be _removeIndex(hash: Array[U8] val) =>
    try
      _entryCheckout.remove(hash)?
      match _index
        | let index: Index =>
          index.remove(hash)?
      end
    else
      None
    end
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
              let cb' = {(id: ((USize, USize) | SectionWriteError)) (hash: Array[U8] val = block.hash, blockCache: BlockCache[B] = this) =>
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

  be get(hash: Array[U8] val, cb: {((Block[B] | SectionReadError | InvalidCacheError | BlockNotFound))} val) =>
    match _index
      | None =>
        cb(InvalidCacheError)
        return
      | let index: Index =>
        try
          match index.find(hash)?
            | let entry: IndexEntry =>
              let cb' = {(data: (Array[U8] val | SectionReadError)) =>
                match data
                  | SectionReadError =>
                    cb(SectionReadError)
                  | let data': Array[U8] val =>
                    let block: Block[B] = Block[B]._withHash(data', hash)
                    cb(block)
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


  be remove(hash: Array[U8] val, cb: {((None | SectionDeallocateError | InvalidCacheError | BlockNotFound))} val) =>
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
    let timer: Timer iso = Timer(consume saver, 250000, 250000)
    _curTimer = timer
    _timers(consume timer)




class IndexSaver is TimerNotify
  let _cb: {()} val
  new iso create(cb:{()} val) =>
    _cb = cb
  fun apply(timer: Timer, count: U64): Bool =>
    _cb()
    false
