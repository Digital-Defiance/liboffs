use "files"
use "collections"
use "json"

primitive SectionReadError
primitive SectionWriteError
primitive SectionDeallocateError

class Fragment
  var start: USize
  var finish: USize
  new create(start': USize = 0, finish': USize = 0) =>
    start = start'
    finish = finish'
  fun ref toJSON(): JsonObject =>
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
  var _path: FilePath
  var _size: USize
  var _fragments: (List[Fragment] | None) = None

  new create(path': FilePath, size: USize, id': USize = 0) =>
    _path = path'
    _size = size
    id = id'
    let fragments: List[Fragment] = List[Fragment] (1)
    fragments.push(Fragment(0, _size - 1))
    _fragments = fragments

  new withFragments(path': FilePath, size: USize, id': USize = 0, array: JsonArray)? =>
    _path = path'
    _size = size
    id = id'
    fragmentsToJSON(array)?

  fun ref _final() =>
    close()

  fun ref close() =>
    match _file
      | let file : File =>
        file.dispose()
        _file = None
    end

  fun ref fragmentsToJSON(): JsonArray =>
    let data = Array[JsonType](_fragments.size())
    for fragment in bucket'.values() do
      data.push(fragment.toJSON())
    end
    JsonArray.from_array(data)

  fun ref fragmentsFromJSON(array: JsonArray)? =>
    let fragments': List[Fragment] = List[Fragment] (array.data.size())
    for fragment in array.data.values() do
      fragments'.push(Fragment.fromJSON(fragment as JsonObject)?)
    end
    _fragments = fragments'

  be deallocate(index: USize, cb: {((None | SectionDeallocateError))} val) =>
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
              return
            elseif (index < frag.finish) then
              if (index >= frag.start) then //Someone tried to deallocate free space
                cb(None)
                return
              else
                break
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
          cb(SectionDeallocateError)
          return
        end
    end
    cb(None)
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

  be write(block: Block[B], cb: {((USize | SectionWriteError))} val) =>
    match try _nextIndex()? else None end
      | None =>
        cb(SectionWriteError)
      | let index : USize =>
        let file: (File | SectionWriteError) = match _file
          | None =>
            match CreateFile(_path)
              | let file': File => file'
              | FileError =>
                SectionWriteError
            else
              SectionWriteError
            end
          | let file' : File => file'
        end
        match file
          | SectionWriteError =>
            cb(SectionWriteError)
          | let file': File =>
            let byte: ISize = (index * BlockSize[B]()).isize()
            file'.seek(byte)
            let ok = file'.write(block.data)
            if (ok) then
              cb(index)
            else
              cb(SectionWriteError)
            end
        end
    end

  be read(index: USize, cb: {((Array[U8] val | SectionReadError))} val) =>
   let file : (File | SectionReadError) = match _file
    | None =>
      match CreateFile(_path)
        | let file': File => file'
      else
        SectionReadError
      end
    | let file' : File =>
      file'
    end
    match file
      | SectionReadError =>
        cb(SectionReadError)
      | let file': File => file
        let byte: ISize = (index * BlockSize[B]()).isize()
        file'.seek(byte)
        let data: Array[U8] val = file'.read(BlockSize[B]())
        cb(data)
    end
