use "collections"
use "json"
use "files"
use "Buffer"

primitive GetBit
  fun apply(data: Buffer box, index: USize = 0) : Bool ? =>
    if index >= (data.size() * 8) then
      error
    end
    let byte: USize = index / 8 // which byte in the array
    let byteIndex: USize = index % 8 // index of the bit in the bytes
    ((data(byte)? and (1 << (byteIndex.u8() - 1))) != 0)

primitive BufferToJson
  fun apply(data: Buffer val) : JsonArray =>
    let array: Array[JsonType] = Array[JsonType](data.size())
    for value in data.values() do
      array.push(value.i64())
    end
    JsonArray.from_array(array)

primitive BufferFromJson
  fun apply(array: JsonArray val): Buffer val ? =>
    recover
      let data: Array[U8] ref = Array[U8](array.data.size())
      for value in array.data.values() do
        data.push((value as I64).u8())
      end
      Buffer(data)
    end
class IndexEntry
  let hits: FibonacciHitCounter ref
  let hash: Buffer val
  var sectionId: USize
  var sectionIndex: USize

  new create(hash': Buffer val, sectionId': USize = 0, sectionIndex': USize = 0) =>
    hash = hash'
    sectionId = sectionId'
    sectionIndex = sectionIndex'
    hits = FibonacciHitCounter

  new from(hash': Buffer val, sectionId': USize, sectionIndex': USize, hits': FibonacciHitCounter) =>
    hash = hash'
    sectionId = sectionId'
    sectionIndex = sectionIndex'
    hits = hits'

  new fromJSON (obj: JsonObject val)? =>
    hits = FibonacciHitCounter.fromJSON(obj.data("hits")? as JsonObject val)?
    hash = BufferFromJson(obj.data("hash")? as JsonArray val)?
    sectionId = (obj.data("sectionId")? as I64).usize()
    sectionIndex = (obj.data("sectionIndex")? as I64).usize()

  fun toJSON (): JsonObject =>
    let obj = JsonObject
    obj.data("hits") = hits.toJSON()
    obj.data("hash") = BufferToJson(hash)
    obj.data("sectionIndex") = sectionIndex.i64()
    obj.data("sectionId") = sectionId.i64()
    obj

  fun box eq(that: box->IndexEntry): Bool =>
    hits == that.hits

  fun box ne(that: box->IndexEntry): Bool =>
    not eq(that)

  fun box gt (that: box->IndexEntry): Bool =>
    hits > that.hits

  fun box gte (that: box->IndexEntry): Bool =>
    hits >= that.hits

  fun box lt (that: box->IndexEntry): Bool =>
    hits < that.hits

  fun box lte (that: box->IndexEntry): Bool =>
    hits <= that.hits

class IndexNode
  var bucket: (List[IndexEntry] | None)
  var left: (IndexNode | None)
  var right: (IndexNode | None)

  new _create(bucket': (List[IndexEntry] | None) = None, left': (IndexNode | None) = None, right': (IndexNode | None) = None) =>
    bucket = bucket'
    left = left'
    right = right'

  new fromJSON(obj: JsonObject val)? =>
    bucket = match obj.data("bucket")?
      | None => None
      | let arr: JsonArray val =>
        let bucket' : List[IndexEntry] = List[IndexEntry]
        for entry in arr.data.values() do
          bucket'.push(IndexEntry.fromJSON(entry as JsonObject val)?)
        end
        bucket'
    end
    left = match obj.data("left")?
      | None => None
      | let obj': JsonObject val => IndexNode.fromJSON(obj')?
      else
        error
    end
    right = match obj.data("right")?
      | None => None
      | let obj': JsonObject val => IndexNode.fromJSON(obj')?
      else
        error
    end
  fun ref toJSON () : JsonObject  =>
    let obj = JsonObject
    obj.data("bucket") = match bucket
      | None => None
      | let bucket': List[IndexEntry] =>
          let data = Array[JsonType](bucket'.size())
          for entry in bucket'.values() do
            data.push(entry.toJSON())
          end
          JsonArray.from_array(data)
    end
    obj.data("left") = match left
      | None => None
      | let left': IndexNode => left'.toJSON()
    end
    obj.data("right") = match right
      | None => None
      | let right': IndexNode => right'.toJSON()
    end
    obj


class Index
  var _root: IndexNode
  let _bucketSize: USize
  let _path: FilePath
  let _ranks: Map[U64, Array[IndexEntry]]

  new create(bucketSize': USize, path': FilePath)? =>
    _bucketSize = bucketSize'
    _root = IndexNode._create(List[IndexEntry](_bucketSize))
    let path = FilePath(path', "index/")?
    path.mkdir()
    _path = FilePath(path, ".index")?
    _ranks = Map[U64, Array[IndexEntry]]

    match OpenFile(_path)
      | let indexFile: File =>
        let obj: JsonObject val = recover
          let text: String = indexFile.read_string(indexFile.size())
          let doc: JsonDoc val = recover val JsonDoc.>parse(text)? end
          doc.data as JsonObject val
        end
        _root = IndexNode.fromJSON(obj.data("root")? as JsonObject val)?
    end


  new from(root': IndexNode, bucketSize': USize, path': FilePath)? =>
    _root = root'
    _bucketSize = bucketSize'
    let path = FilePath(path', ".index")?
    path.mkdir()
    _path = path
    _ranks = Map[U64, Array[IndexEntry]]

  new fromJSON(obj: JsonObject val, path': FilePath)? =>
    _root = IndexNode.fromJSON(obj.data("root")? as JsonObject val)?
    _bucketSize = (obj.data("bucketSize")? as F64).usize()
    let path = FilePath(path', ".index")?
    path.mkdir()
    _path = path
    _ranks = Map[U64, Array[IndexEntry]]
    let entries: List[IndexEntry] = List[IndexEntry]
    for entry in entries.values() do
      try
        let rank: Array[IndexEntry] = _ranks(entry.hits.fib())?
        rank.push(entry)
      else
        let rank: Array[IndexEntry] = Array[IndexEntry](1)
        rank.push(entry)
        _ranks.insert(entry.hits.fib(), rank)
      end
    end

  fun ref toJSON(): JsonObject =>
    let obj = JsonObject
    obj.data("root") = _root.toJSON()
    obj.data("bucketSize") = _bucketSize.f64()
    obj

  fun ref save() =>
    let obj: JsonObject = toJSON()
    let doc: JsonDoc = JsonDoc
    doc.data = obj
    match CreateFile(_path)
      | let file: File =>
        let text: String = doc.string()
        file.set_length(text.size())
        file.write(text)
        file.dispose()
    end

  fun ref add(entry: IndexEntry, node': (IndexNode| None) = None, index: USize = 0) ? =>
    let node: IndexNode = match node'
      | None => _root
      | let node: IndexNode => node
    end
    match node.bucket
      | None => // this is an internal Node
        if GetBit(entry.hash, index + 1)? then
          add(entry, node.right, index + 1)?
        else
          add(entry, node.left, index + 1)?
        end
      | let bucket': List[IndexEntry] =>
        for entry' in bucket'.values() do
          if entry.hash == entry'.hash then //Update
            _increment(entry')
            return
          end
        end
        if (bucket'.size() < _bucketSize) then
          bucket'.push(entry)
        else
          _split(node, index)?
          add(entry, node, index)?
        end
        try
          let rank: Array[IndexEntry] = _ranks(entry.hits.fib())?
          rank.push(entry)
        else
          let rank: Array[IndexEntry] = Array[IndexEntry](1)
          rank.push(entry)
          _ranks.insert(entry.hits.fib(), rank)
        end
    end

  fun ref _increment(entry: IndexEntry) =>
    if entry.hits.increment() then
      try
        var rank: Array[IndexEntry] = _ranks(entry.hits.fib() - 1)?
        for i in Range(0, rank.size()) do
          if rank(i)?.hash == entry.hash then
            rank.delete(i)?
            break
          end
        end
        try
          rank = _ranks(entry.hits.fib())?
          rank.push(entry)
        else
          rank = Array[IndexEntry](1)
          rank.push(entry)
          _ranks.insert(entry.hits.fib(), rank)
        end
      end
    end


  fun ref get(hash: Buffer val, node': (IndexNode | None) = None, index: USize = 0): IndexEntry ? =>
    let node : IndexNode = match node'
      | None => _root
      | let node: IndexNode => node
    end
    match node.bucket
      | None =>
        if GetBit(hash, index + 1)? then
          get(hash, node.right, index + 1)?
        else
          get(hash, node.left, index + 1)?
        end
      | let bucket': List[IndexEntry] =>
        for entry' in bucket'.values() do
          if hash == entry'.hash then
            _increment(entry')
            return entry'
          end
        end
        let entry: IndexEntry = IndexEntry(hash)
        add(entry, node', index)?
        return entry
    end

  fun ranks(): Map[U64, Array[IndexEntry]] box =>
    _ranks

  fun ref find(hash: Buffer val, node': (IndexNode | None) = None, index: USize = 0): (IndexEntry | None) ? =>
    let node : IndexNode = match node'
      | None => _root
      | let node: IndexNode => node
    end
    match node.bucket
      | None =>
        if GetBit(hash, index + 1)? then
          find(hash, node.right, index + 1)?
        else
          find(hash, node.left, index + 1)?
        end
      | let bucket': List[IndexEntry] =>
        for entry' in bucket'.values() do
          if hash == entry'.hash then
            _increment(entry')
            return entry'
          end
        end
        None
    end

  fun ref remove(hash: Buffer val, node': (IndexNode | None) = None, index: USize = 0) ? =>
    let node : IndexNode = match node'
      | None => _root
      | let node: IndexNode => node
    end
    match node.bucket
      | None =>
        if GetBit(hash, index + 1)? then
          remove(hash, node.right, index + 1)?
        else
          remove(hash, node.left, index + 1)?
        end
      | let bucket': List[IndexEntry] =>
        var i : USize = 0
        for entry' in bucket'.values() do
          if hash == entry'.hash then
            bucket'.remove(i)?
            try
              let ranks': Array[IndexEntry] = _ranks(entry'.hits.fib())?
              for j in Range(0, ranks'.size()) do
                if ranks'(j)?.hash == entry'.hash then
                  ranks'.delete(j)?
                  break
                end
              end
            end
            break
          end
          i = i + 1
        end
    end

    fun ref _split(node: IndexNode, index: USize) ? =>
      match node.bucket
        | let bucket': List[IndexEntry] =>
          node.left = IndexNode._create()
          match node.left
            | let left: IndexNode => left.bucket = List[IndexEntry](_bucketSize)
          end
          node.right = IndexNode._create()
          match node.right
            | let right: IndexNode => right.bucket = List[IndexEntry](_bucketSize)
          end
          node.bucket = None
          for entry' in bucket'.values() do
            add(entry', node, index)?
          end
        end

    fun ref size(node': (IndexNode | None) = None) : USize =>
      let node = match node'
        | None => _root
        | let node : IndexNode => node
      end
      match node.bucket
        | None =>
          return size(node.right) + size(node.left)
        | let bucket': List[IndexEntry] =>
          bucket'.size()
      end

    fun ref list(node': (IndexNode | None) = None) : List[IndexEntry] =>
      let node = match node'
        | None => _root
        | let node : IndexNode => node
      end
      match node.bucket
        | None =>
          let listRight: List[IndexEntry] = list(node.right)
          listRight.prepend_list(list(node.left))
          listRight
        | let bucket': List[IndexEntry] =>
          bucket'.clone()
      end

    fun ref array(node': (IndexNode | None) = None) : Array[IndexEntry] =>
      let node = match node'
        | None => _root
        | let node : IndexNode => node
      end
      match node.bucket
        | None =>
          match (node.left, node.right)
            | (let left': IndexNode, let right': IndexNode) =>
              let arr1: Array[IndexEntry] = array(left')
              let arr2: Array[IndexEntry] = array(right')
              arr1.concat(arr2.values())
              arr1
          else
            Array[IndexEntry](0)
          end
        | let bucket': List[IndexEntry] =>
          let arr: Array[IndexEntry] = Array[IndexEntry](bucket'.size())
          for entry in bucket'.values() do
            arr.push(entry)
          end
          arr
      end
