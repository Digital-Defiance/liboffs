use "files"
use "collections"

primitive SectionReadError
primitive SectionWriteError
actor Section [B: BlockType]
  var _id: USize
  var _file: (File | None) = None
  var _path: FilePath
  var _size: USize
  var _fragments: (List[(USize, USize)] | None) = None
  var _index: USize
  new create(path': FilePath, size: USize, id': USize = 0, index': USize = 0) =>
    _path = path'
    _size = size
    _id = id'
    _index = index'
    _fragments = [(0, _size * BlockSize[B]())]
  fun ref nextIndex(): USize val=>
    _index = _index + 1
  be write(id: USize, block: Block[B], cb: {((Bool | SectionWriteError))} val) =>
    let file: (File | SectionWriteError) = match _file
      | None =>
        match OpenFile(_path)
          | let file': File => file'
        else
          SectionWriteError
        end
      | let file' : File => file'
    end
    match file
      | SectionWriteError => cb(SectionWriteError)
      | let file': File =>
        let byte: ISize = (id * BlockSize[B]()).isize()
        file'.seek(byte)
        let ok = file'.write(block.data)
        cb(ok)
    end

  be read(id: USize, block: Block[B], cb: {((Array[U8] val | SectionReadError))} val) =>
   let file : (File | SectionReadError) = match _file
    | None =>
      match OpenFile(_path)
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
        let byte: ISize = (id * BlockSize[B]()).isize()
        file'.seek(byte)
        let data: Array[U8] val = file'.read(BlockSize[B]())
        cb(data)
    end
