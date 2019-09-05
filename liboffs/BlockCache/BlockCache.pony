use "files"
use "time"
use "LRUCache"

primitive InvalidCacheError
primitive BlockNotFound

actor BlockCache [B: BlockType]
  var _path: (FilePath | None) = None
  let _timers: Timers =  Timers
  var _curTimer: (Timer | None) = None
  var _index: (Index | None) = None
  var _sections: (Sections[B] | None) = None
  let _blocks: LRUCache[Array[U8] val, Block[B]] = LRUCache[USize,Block[B]](20)// TODO: How large should the cache be?

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
        _index = Index(path)
        _sections = Sections(path, 25)// TODO: How large should a section be?
    end

  fun ref put(block: Block[B], cb: {((None | SectionWriteError | InvalidCacheError ))} val) =>
      match _index
        | None =>
          cb(InvalidCacheError)
          return
        | let index: Index =>
          let entry: IndexEntry =  index.get(block.hash)
          if index.sectionId == 0 then
            let cb' = {(id: ((USize, Usize) | SectionWriteError)) =>
              index.save()
              match id
                | let id: (USize, USize) =>
                  entry.sectionId = id._1
                  entry.sectionIndex = id._2
                  cb(err)
                | SectionWriteError =>
                  index.remove(block.hash)
                  cb(SectionWriteError)
              end
            } val
            _sections.write(block, cb')
            _saveIndex()
          else
              cb(None)
          end
      end

  fun ref get(hash: Array[U8] val, cb: {((Block[B] | SectionReadError | InvalidCacheError | BlockNotFound))} val) =>
    match _blocks(hash)
      | let block: Block[B] =>
        cb(block)
      | None =>
        match _index
          | None =>
            cb(InvalidCacheError)
            return
          | let index: Index =>
              match index.find(hash)
                | let entry: IndexEntry =>
                  let cb' = {(data: (Array[U8] val | SectionReadError)) =>
                    match data
                      | SectionReadError =>
                        cb(SectionReadError)
                      | let data': Array[U8] val =>
                        let block: Block[B] = Block[B]._withHash(data', hash)
                        _blocks(hash) = block
                        cb(block)
                    end
                  } val
                  _sections.read(entry, cb)
                  _saveIndex()
                | None =>
                  cb(BlockNotFound)
                  return
              end
        end
    end


  fun ref remove(hash: Array[U8] val, cb: {((None | SectionDeallocateError | InvalidCacheError | BlockNotFound))} val) =>
    match _index
      | None =>
        cb(InvalidCacheError)
        return
      | let index: Index =>
          match index.find(hash)
            | let entry: IndexEntry =>
              let cb' = {(err: (None | SectionDeallocateError)) =>
                match err
                  | SectionReadError =>
                    cb(SectionReadError)
                  | None  =>
                    index.remove(hash)
                    cb(None)
                    _saveIndex()
                end
              } val
              _sections.deallocate(entry, cb')
            | None =>
              cb(BlockNotFound)
              return
          end
    end

  fun ref _saveIndex() =>
    match _curTimer
      | let curTimer': Timer =>
        _timers.cancel(curTimer')
    end
    match _index
      | let index: Index =>
        let saver: IndexSaver = IndexSaver({() => index.save()} val)
        let timer : Timer = Timers(saver, 250000, 250000)
        _curTimer = timer
        _timers(consume timer)
    end



class IndexSaver is TimerNotify
  let _cb: {()} val
  new iso create(cb:{()} val) =>
    _cb = cb
  fun apply(timer: Timer, count: U64): Bool =>
    _cb()
    false
