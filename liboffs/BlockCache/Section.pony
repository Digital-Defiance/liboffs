use "files"
use "collections"
use "json"
use "time"
use "Buffer"
use "Exception"

class Fragment
  var start: USize
  var finish: USize
  new create(start': USize = 0, finish': USize = 0) =>
    start = start'
    finish = finish'
  fun toJSON(): JsonObject =>
    let obj = JsonObject
    obj.data("start") = start.f64()
    obj.data("finish") = finish.f64()
    obj
  new fromJSON(obj: JsonObject)? =>
    start = (obj.data("start")? as I64).usize()
    finish = (obj.data("finish")? as I64).usize()



actor Section [B: BlockType]
  let id: USize
  var _file: (File | None) = None
  var _metaPath: (FilePath | None) = None
  var _path: (FilePath | None) = None
  var _size: USize
  var _fragments: (List[Fragment] | None) = None
  var _saver: (Timer iso! | None) = None
  let _timers: Timers = Timers

  new create(path': FilePath, metaPath: FilePath, size: USize, id': USize = 0) =>
    _size = size
    id = id'

    try
      _path = FilePath.from(path', id.string())?
      _metaPath = FilePath.from(metaPath, id.string())?
      match OpenFile(_metaPath as FilePath)
        | let metaFile: File =>
          let text: String = metaFile.read_string(metaFile.size())
          let doc: JsonDoc = JsonDoc
          doc.parse(text)?
          let array: JsonArray = doc.data as JsonArray
          if array.data.size() > 0 then
            let fragments': List[Fragment] = List[Fragment] (array.data.size())
            for fragment in array.data.values() do
              fragments'.push(Fragment.fromJSON(fragment as JsonObject)?)
            end
            _fragments = fragments'
          else
            _fragments = None
          end
        else
          let fragments: List[Fragment] = List[Fragment] (1)
          fragments.push(Fragment(0, _size - 1))
          _fragments = fragments
      end
    else
      let fragments: List[Fragment] = List[Fragment] (1)
      fragments.push(Fragment(0, _size - 1))
      _fragments = fragments
    end




  be _saveFragments() =>
    let arr: JsonArray =  match _fragments
      | let fragments: List[Fragment] =>
        let data = Array[JsonType](fragments.size())
        for fragment in fragments.values() do
          data.push(fragment.toJSON())
        end
        JsonArray.from_array(data)
      | None =>
        JsonArray.from_array(Array[JsonType](0))
    end
    let doc: JsonDoc= JsonDoc
    doc.data = arr
    match _metaPath
      | let metaPath: FilePath =>
        match CreateFile(metaPath)
          | let file: File =>
            let text = doc.string()
            file.set_length(text.size())
            file.write(text)
            file.flush()
            file.dispose()
        end
    end

  be deallocate(index: USize, cb: {((None | Exception))} val) =>
    match _fragments
      | None =>
        let fragments: List[Fragment] = List[Fragment] (1)
        fragments.push(Fragment(index, index + 1))
        _fragments = fragments
      | let fragments: List[Fragment] =>
        try
          var i: USize = 0
          let lastNode: ListNode[Fragment] = fragments.tail()?
          var last: Fragment = lastNode()?
          for frag in fragments.values() do
            if (index == frag.finish) then //Someone tried to deallocate free space
              cb(None)
              _save()
              return
            elseif (index < frag.finish) then
              if (index >= frag.start) then //Someone tried to deallocate free space
                cb(None)
                _save()
                return
              end
            else
              i  = i + 1
              last = frag
            end
          end
          if (index == (last.finish + 1)) then
            if ((i < (fragments.size() - 1)) and (fragments(i + 1)?.start == (last.finish + 1))) then //join newly connected ranges
              last.finish = fragments(i + 1)?.finish
              fragments.remove(i + 1)?
            else
              last.finish = index
            end
          else
            let pt1: List[Fragment] = fragments.take(i)
            pt1.push(Fragment(index,index))
            let pt2 :List[Fragment] = fragments.drop(i)
            pt1.concat(pt2.values())
            _fragments = pt1
          end
        else
          cb(Exception("Section Deallocate Error"))
          return
        end
    end
    cb(None)
    _save()

  fun ref _nextIndex(): (USize val | None) ? =>
    match _fragments
      | None => None
      | let fragments: List[Fragment] =>
        var index: USize = -1
        var nxt: USize = -1
        for frag in fragments.values() do
          index = index + 1
          if (frag.start == frag.finish) then
            fragments.remove(index)?
            nxt = frag.start
            break
          else
            nxt = frag.start
            frag.start = frag.start + 1
            break
          end
        end
        if (fragments.size() == 0) then
          _fragments = None
        end
        if nxt == -1 then
          None
        else
          nxt
        end
    end

  fun full(): Bool =>
    match _fragments
    | None =>
      true
    else
      false
    end

  be write(block: Block[B], cb: {(((USize, Bool) | Exception))} val) =>
    match try _nextIndex()? else None end
      | None =>
        cb(Exception("Section Full"))
      | let index : USize =>
        let file: (File | Exception) = match _file
          | None =>
            match try CreateFile(_path as FilePath) else FileError end
              | let file': File => file'
              | FileError =>
                Exception("File Error")
            else
              Exception("Section Write Error")
            end
          | let file' : File => file'
        end
        match file
        | let err: Exception =>
            cb(err)
        | let file': File =>
            let byte: ISize = (index * BlockSize[B]()).isize()
            file'.seek(byte)
            let ok = file'.write(block.data.data)
            file'.flush()
            if (ok) then
              cb((index, full()))
              _save()
            else
              cb(Exception("Section Write Error"))
            end
        end
    end

  be read(index: USize, cb: {((Buffer val | Exception))} val) =>
   let file : (File | Exception) = match _file
    | None =>
      match try CreateFile(_path as FilePath) else FileError end
        | let file': File => file'
      else
        Exception("File Error")
      end
    | let file' : File =>
      file'
    end
    match file
      | let err: Exception =>
        cb(err)
      | let file': File => file
        let byte: ISize = (index * BlockSize[B]()).isize()
        file'.seek(byte)
        let size: USize = BlockSize[B]()
        let data: Buffer val = recover Buffer(file'.read(size)) end
        if data.size() < BlockSize[B]() then
          cb(Exception("Section Read Invalid - Section " + id.string() + " index " + index.string()))
        else
          cb(data)
        end
    end
  fun ref _save() =>
    match _saver
    | let saver : Timer iso! =>
        _timers.cancel(saver)
    end
    let saver: SectionSaver iso = SectionSaver({() (section: Section[B] = this)=>
      section._saveFragments()
    } val)
    let timer = Timer(consume saver, 250000, 250000)
    _saver = timer
    _timers(consume timer)

class SectionSaver is TimerNotify
  let _cb: {()} val
  new iso create(cb: {()} val) =>
    _cb = cb
  fun apply(timer: Timer, count: U64): Bool =>
    _cb()
    false
