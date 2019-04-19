use "files"
use "collections"

primitive SectionReadError
primitive SectionWriteError

class Fragment
  var start: USize
  var finish: USize
  new create(start': USize = 0, finish': USize = 0) =>
    start = start'
    finish = finish'

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
    fragments.push(Fragment(0, _size))
    _fragments = fragments

  fun ref deallocate(index: USize) =>
    match _fragments
      | None =>
        let fragments: List[Fragment] = List[Fragment] (1)
        fragments.push(Fragment(index, index))
        _fragments = fragments
      | let fragments: List[Fragment] =>
        var i: USize = 0
        for frag in fragments.values() do
          if (index > frag.start) then
            if (index == (frag.start + 1)) then
              frag.start = index
            else
              let pt1: List[Fragment] = fragments.take(i)
              pt1.push(Fragment(index,index))
              let pt2 :List[Fragment] = fragments.drop(i)
              pt1.concat(pt2.values())
              _fragments = pt1
            end
            break
          end
          i  = i + 1
        end
    end
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
