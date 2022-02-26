use "collections"
use "files"
use "json"
use "LRUCache"
use "time"
use "Buffer"
use "Exception"

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
  let _maxTupleSize: USize

  new create(path': FilePath, size: USize, maxTupleSize: USize) =>
    _maxTupleSize = maxTupleSize
    _roundRobin = recover List[USize](_maxTupleSize) end
    _size = size
    try
      let path =  FilePath.from(path', "sections/")?
      _dataPath = FilePath.from(path, "data/")?
      _metaPath = FilePath.from(path, "meta/")?
      match (_dataPath, _metaPath)
        | (let dataPath: FilePath, let metaPath: FilePath) =>
          dataPath.mkdir()
          metaPath.mkdir()
          let metaDir = Directory(metaPath)?
          _nextId = metaDir.entries()?.size() + 1
      end
      _robinPath = FilePath.from(path, ".robin")?

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
        while _roundRobin.size() < _maxTupleSize do
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
    _roundRobin = _roundRobin.filter({(section) : Bool => section != sectionId} val)
    while _roundRobin.size() < _maxTupleSize do
      try
        let id: USize = _nextId = _nextId + 1
        let section': Section[B] = Section[B]((_dataPath as FilePath), (_metaPath as FilePath), _size, id)
        _roundRobin.unshift(id)
        _sections(id) = section'
        _save()
      else
        None
      end
    end

  be _free(sectionId: USize) =>
    if _roundRobin.contains[USize](sectionId) == false then
      _roundRobin.unshift(sectionId)
      _save()
    end

  be write(block: Block[B], cb: {(((USize, USize) | Exception))} val) =>
    try
      let sectionId: USize = _roundRobin.shift()?
      _roundRobin.push(sectionId)
      let section: Section[B] = match _sections(sectionId)
        | None =>
          let section': Section[B] = Section[B]((_dataPath as FilePath), (_metaPath as FilePath), _size, sectionId)
          _sections(sectionId) = section'
          section'
        | let section': Section[B] =>
          section'
      end
      let cb' = {(index: ((USize, Bool) | Exception)) (sectionId, sections: Sections[B] = this) =>
        match index
          | (let index': USize, let full: Bool) =>
            if full then
              sections._full(sectionId)
            end
            cb((sectionId, index'))
          | let err: Exception =>
            cb(err)
        end
      } val
      section.write(block, cb')
    else
      cb(Exception("Section Write Error"))
    end

  be read(sectionId: USize, sectionIndex: USize, cb: {((Buffer val | Exception))} val) =>
    match _sections(sectionId)
      | let section: Section[B] =>
        let cb' = {(data:(Buffer val | Exception)) =>
          cb(data)
        } val
        section.read(sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(sectionId)?
          let cb' = {(data:(Buffer val | Exception)) =>
            cb(data)
          } val
          section.read(sectionIndex, cb')
        else
          cb(Exception("Section Read Error"))
        end
    end

  be deallocate(sectionId: USize, sectionIndex: USize, cb: {((None | Exception))} val) =>
    match _sections(sectionId)
      | let section: Section[B] =>
        let cb' = {(err: (None | Exception)) (sectionId, sections: Sections[B] = this)=>
          match err
            | None =>
              sections._free(sectionId)
          end
          cb(err)
        } val
        section.deallocate(sectionIndex, cb')
      | None =>
        try
          let section: Section[B] = _getSection(sectionId)?
          let cb' = {(err: (None | Exception)) (sections: Sections[B] = this) =>
            match err
              | None =>
                sections._free(sectionId)
            end
            cb(err)
          } val
          section.deallocate(sectionIndex, cb')
        else
          cb(Exception("Section Deallocate Error"))
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
          let text: String = doc.string()
          file.set_length(text.size())
          file.write(text)
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
