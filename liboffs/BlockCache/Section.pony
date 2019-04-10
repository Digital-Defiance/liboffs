use "files"
use "collections"

actor Section
  var _id: USize
  var _file: (File | None)
  var _path: String
  var _blockSize: USize
  var _size
  new create(path': String, blockSize': BlockSize, size: USize) =>
    path = path'
    _blockSize = blockSize'
    _size = size

  be write(id: USize, block: Block, cb: {(Bool))} val) =>
    let byte: ISize = (id * _blockSize).isize()
    _file.position(byte)
    let ok = _file.write(block.data)
    cb(ok)

  be read(id: USize, block: Block, cb: {(Array[U8]))} val) =>
    let byte: ISize = (id * _blockSize).isize()
    _file.position(byte)
    let data: Array[U8] = _file.read(_blockSize)
    cb(consume data)
