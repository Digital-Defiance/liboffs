use "collections"
use "files"
use "json"
use "LRUCache"
use "time"

actor Sections [B:BlockType]
  var _nextId: USize  = 1
  var _roundRobin: List[USize]
  var _dataPath: (FilePath | None) = None
  var _metaPath: (FilePath | None) = None
  var _robinPath: (FilePath | None) = None
  let _size: USize
  let _sections: LRUCache[USize, Section[B]] = LRUCache[USize, Section[B]](10)
  let _timers: Timers = Timers
  var _saver: (Timer iso! | None) = None
  new create(path': FilePath, size: USize) =>
    _roundRobin = recover List[USize](5) end
    _size = size
    try
      let path =  FilePath(path', "sections/")?
      _dataPath = FilePath(path, "data/")?
      _metaPath = FilePath(path, "meta/")?
      match (_dataPath, _metaPath)
        | (let dataPath: FilePath, let metaPath: FilePath) =>
          dataPath.mkdir()
          metaPath.mkdir()
      end
      _robinPath = FilePath(path, ".robin")?

      match OpenFile((_robinPath as FilePath))
        | let robinFile: File =>
          let text: String = robinFile.read_string(robinFile.size())
          let doc: JsonDoc = JsonDoc
        doc.parse(text)?
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
    else
      None
    end
    match (_dataPath, _metaPath)
      | (let dataPath: FilePath, let metaPath: FilePath) =>
        while _roundRobin.size() < 5 do
          let id: USize = (_nextId = _nextId + 1)
          let section: Section[B] = Section[B](dataPath, metaPath, _size, id)
          _roundRobin.push(id)
          _sections(id) = section
        end
    end


  fun ref _getSection(id: USize): Section[B]? =>
    let section: Section[B] = Section[B]((_dataPath as FilePath),  (_metaPath as FilePath), _size, id)
    _roundRobin.push(id)
    _sections(id) = section
    section
  be _full(sectionId: USize) =>
    _roundRobin = _roundRobin.filter({(section) : Bool => section == sectionId} val)
    let id: USize = _nextId = _nextId + 1
    try
      let section': Section[B] = Section[B]((_dataPath as FilePath), (_metaPath as FilePath), _size, id)
      _roundRobin.unshift(id)
      _sections(id) = section'
    else
      None
    end
  be write(block: Block[B], cb: {(((USize, USize) | SectionWriteError))} val) =>
    try
      let sectionId: USize = _roundRobin.shift()?
      _roundRobin.push(sectionId)
      match _sections(sectionId)
        | None =>
          cb(SectionWriteError)
        | let section: Section[B] =>
          let cb' = {(index: ((USize, Bool) | SectionWriteError)) (sectionId, sections: Sections[B] = this) =>
            match index
            | (let index': USize, let full: Bool) =>
                if full then
                  sections._full(sectionId)
                end
                cb((sectionId, index'))
              else
                cb(SectionWriteError)
            end
          } val
          section.write(block, cb')
      end
    else
      cb(SectionWriteError)
    end

  be read(sectionId: USize, sectionIndex: USize, cb: {((Array[U8] val | SectionReadError))} val) =>
    match _sections(sectionId)
      | let section: Section[B] =>
        let cb' = {(data:(Array[U8] val | SectionReadError)) =>
          cb(data)
        } val
        section.read(sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(sectionId)?
          let cb' = {(data:(Array[U8] val | SectionReadError)) =>
            cb(data)
          } val
          section.read(sectionIndex, cb')
        else
          cb(SectionReadError)
        end
    end

  be deallocate(sectionId: USize, sectionIndex: USize, cb: {((None | SectionDeallocateError))} val) =>
    match _sections(sectionId)
      | let section: Section[B] =>
        let cb' = {(err: (None | SectionDeallocateError)) =>
          cb(err)
        } val
        section.deallocate(sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(sectionId)?
          let cb' = {(err: (None | SectionDeallocateError)) =>
            cb(err)
          } val
          section.deallocate(sectionIndex, cb')
        else
          cb(SectionDeallocateError)
        end
    end
  be _saveRoundRobin() =>
    let arr: Array[JsonType] = Array[JsonType](_roundRobin.size())
    for id in _roundRobin.values() do
      arr.push(id.f64())
    end
    let doc: JsonDoc = JsonDoc
    doc.data = JsonArray.from_array(arr)
    try
      match CreateFile((_robinPath as FilePath))
        | let file: File =>
          file.write(doc.string())
          file.dispose()
      end
    else
      None
    end
  fun ref _save() =>
    match _saver
    | let saver : Timer iso! =>
        _timers.cancel(saver)
    end
    let saver: SectionSaver iso = SectionSaver({() (sections: Sections[B] = this) =>
      sections._saveRoundRobin()
    } val)
    let timer = Timer(consume saver, 250000, 250000)
    _saver = timer
    _timers(consume timer)
