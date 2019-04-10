primitive GetBit
  apply(data: Array[U8] val, index: USize = 0) : Bool ? =>
    let byte: USize = index / 8 // which byte in the array
    let byteIndex: USize = index % 8 // index of the bit in the bytes
    if ((data.size() < byte) and (byteIndex != 0)) then
      false
    elseif (data(byte)? and F64(2).pow(7 - byteIndex).u8()) then
      true
    else
      false
    end


class IndexEntry
  let _hits: FibonacciHitCounter ref
  let _key: String val
  var _sectionId: U64 val

  new create(key': String val, sectionId: U64 val) =>
    _key = key'
    _sectorId = 1
    _hits = FibonacciHitCounter

  new from(key': String val, sectionId: U64, hits': FibonacciHitCounter) =>
    _key = key'
    _sectorId = sectionId
    _hits = hits'

  fun box eq(that: box->IndexEntry): Bool =>
    _hits == that._hits

  fun box ne(that: box->IndexEntry): Bool =>
    not eq(that)

  fun box gt (that: box->IndexEntry): Bool =>
    _hits > that._hits

  fun box gte (that: box->IndexEntry): Bool =>
    _hits >= that._hits

  fun box lt (that: box->IndexEntry): Bool =>
    _hits < that._hits

  fun box lte (that: box->IndexEntry): Bool =>
    _hits <= that._hits

class IndexNode
  var _bucket: (List[IndexEntry] | None) = None
  var _left: (IndexNode | Node) = None
  var _right: (IndexNode | None) = None


class Index // BTree
  var _root: IndexNode
  let _bucketSize: USize

  new create(bucketSize': USize) =>
    _bucketSize = bucketSize'
    _root = IndexNode
    _root._bucket = new List[IndexEntry](_bucketSize)

  new from(root': IndexNode, bucketSize': USize) =>
    _root = root'
    _bucketSize = bucketSize'

  fun ref add(entry: IndexEntry, node: IndexNode = _root, index: USize = 0) =>
    match node._bucket
      | None => // this is an internal Node
        index = index + 1
        if GetBit(IndexEntry._key, index) then
          add(entry, node._right, index)
        else
          add(entry, node._left, index)
        end
      | let bucket': List[IndexEntry] =>
        for entry' in bucket'.values() do
          if (entry.key == entry'.key) then //Update
            entry'._hits.increment() // TODO Should I even have and update!?
            return
          end
        end
        if (bucket'.size() < _bucket.size()) then
          entry'._hits.increment()
          bucket'.push(entry)
        else
          _split(node, index)
          add(entry, node, index)
        end
    end

    fun get(key: String val, node: IndexNode = _root, index: USize = 0): (IndexEntry | None) =>
      match node._bucket
        | None =>
          index = index + 1
          if GetBit(key, index) then
            get(key, node._right, index)
          else
            get(entry, node._left, index)
          end
        | let bucket': List[IndexEntry] =>
          for entry' in bucket'.values() do
            if (key == entry'.key) then
              return entry'
            end
          end
          None
      end

    fun ref _split(node: IndexNode, index: USize) =>
      match node._bucket
        | let bucket': List[IndexEntry] =>
          node._left = IndexNode
          node._left._bucket = new List[IndexEntry](_bucketSize)
          node._right = IndexNode
          node._right._bucket = new List[IndexEntry](_bucketSize)
          node._bucket = None
          for entry' in bucket'.values() do
            add(entry', node, index)
          end
        end

      fun size(node: IndexNode = _root) : USize =>
        match node._bucket
          | None =>
            return size(node._right) + size(node._left)
          | let bucket': List[IndexEntry] =>
            bucket.size()
        end

      fun list(node: IndexNode = _root) : List[IndexEntry] =>
        match node._bucket
          | None =>
            return list(node._right).prepend_list(list(node._left))
          | let bucket': List[IndexEntry] =>
            bucket.clone()
        end
