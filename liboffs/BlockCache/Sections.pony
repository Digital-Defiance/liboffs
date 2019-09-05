use "collections"
use "files"
use "json"
use "LRUCache"
use "time"

class Sections [B:BlockType]
  var _nextId: USize  = 1
  var _roundRobin: List[USize] iso
  let _dataPath: FilePath
  let _metaPath: FilePath
  let _robinPath: FilePath
  let _size: USize
  let _sections: LRUCache[USize, Section[B]] iso = recover LRUCache[USize, Section[B]](10) end
  let _timers: Timers = Timers
  var _saver: (Timer | None) = None
  new create(path': FilePath, size: USize) ? =>
    _roundRobin = recover List[USize](5) end
    _size = size
    let path = FilePath(path', "sections/")?
    _dataPath = FilePath(path, "data/")?
    _metaPath = FilePath(path, "meta/")?
    _dataPath.mkdir()
    _metaPath.mkdir()
    _robinPath = FilePath(path, ".robin")

    match OpenFile(_robinPath)
      | let robinFile: File =>
        let text: String = robinFile.read_string(robinFile.size())
        let doc: JsonDoc = JsonDoc
        doc.parse(text)
        try
          let arr: JsonArray = doc.data as JsonArray
          for sectionId in arr.data.values() do
            let id: USize = (sectionId as F64).usize()
            _roundRobin.push(id)
            _nextId = id + 1
          end
        else
          None
        end
    end
    while _roundRobin.size() < 5 do
      let id: USize = (_nextId = _nextId + 1)
      let section: Section[B] = Section[B](_dataPath, _metaPath, _size, id)
      _roundRobin.push(id)
      _sections(id) = section
    end


  fun ref _getSection(id: USize): Section[B] =>
    let section: Section[B] = Section[B](_dataPath, _metaPath, _size, id)
    _roundRobin.push(id)
    _sections(id) = section
    section


  fun ref write(block: Block[B], cb: {(((USize, USize) | SectionWriteError))} val) =>
      let sectionId: USize = _roundRobin.shift()
      _roundRobin.unshift(sectionId)
      match _sections(sectionId)
        | None =>
          cb(SectionWriteError)
        | let section: Section[B] =>
          let cb' = {(index: ((USize, Bool) | SectionWriteError)) (sectionId) =>
            match index
            | (let index': USize, let full: Bool) =>
                if full then
                  _roundRobin = _roundRobin.filter({(section) : Bool => section == sectionId} val)
                  let id: USize = _nextId = _nextId + 1
                  let section': Section[B] = Section[B](_dataPath, _metaPath, _size, id)
                  _roundRobin.push(section')
                  _sections(id) = section'
                end
                cb((sectionId, index'))
              else
                cb(SectionWriteError)
            end
          } val
          section.write(block, cb')
      end

  fun read(entry: IndexEntry, cb: {((Array[U8] val | SectionReadError))} val) =>
    match _sections(entry.sectionId)
      | let section: Section[B] =>
        let cb' = {(data:(Array[U8] val | SectionReadError)) =>
          cb(data)
        } val
        section.read(entry.sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(entry.sectionId)
          let cb' = {(data:(Array[U8] val | SectionReadError)) =>
            cb(data)
          } val
          section.read(entry.sectionIndex, cb')
        else
          cb(SectionReadError)
        end
    end

  fun deallocate(entry: IndexEntry, cb: {((None | SectionDeallocateError))} val) =>
    match _sections(entry.sectionId)
      | let section: Section[B] =>
        let cb' = {(err: (None | SectionDeallocateError)) =>
          cb(err)
        } val
        section.deallocate(entry.sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(entry.sectionId)
          let cb' = {(err: (None | SectionDeallocateError)) =>
            cb(err)
          } val
          section.deallocate(entry.sectionIndex, cb')
        else
          cb(SectionDeallocateError)
        end
    end
  fun _saveRoundRobin() =>
    let arr: Array[F64] = Array[F64](_roundRobin.size())
    for id in _roundRobin.values() do
      arr.push(id.f64())
    end
    let doc: JsonDoc = JsonDoc
    doc.data = JsonArray.from_array(arr)
    match CreateFile(_robinPath)
      | let file: File =>
        file.write(doc.string())
        file.dispose()
    end

  fun ref _save() =>
    match _saver
      | let saver : Timer =>
        _timers.cancel(saver)
    end
    let saver: SectionSaver iso = SectionSaver({() =>
      _saveRoundRobin()
    } val)
    let timer = Timer(saver, 250000, 250000)
    _saver = timer
    _timers(consume timer)
