use "collections"

primitive GetBit
  fun apply(data: ByteSeq, index: USize = 0) : Bool ? =>
    let byte: USize = index / 8 // which byte in the array
    let byteIndex: USize = index % 8 // index of the bit in the bytes
    if ((data.size() < byte) and (byteIndex != 0)) then
      false
    elseif ((data(byte)? and F64(2).pow(7 - byteIndex.f64()).u8()) == 1) then
      true
    else
      false
    end


class IndexEntry
  let hits: FibonacciHitCounter ref
  let key: String val
  var sectionId: USize val

  new _create(key': String val, sectionId': USize val) =>
    key = key'
    sectionId = sectionId'
    hits = FibonacciHitCounter

  new from(key': String val, sectionId': USize, hits': FibonacciHitCounter) =>
    key = key'
    sectionId = sectionId'
    hits = hits'

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

class Index // BTree
  var _root: IndexNode
  let _bucketSize: USize

  new create(bucketSize': USize) =>
    _bucketSize = bucketSize'
    _root = IndexNode._create(List[IndexEntry](_bucketSize))

  new from(root': IndexNode, bucketSize': USize) =>
    _root = root'
    _bucketSize = bucketSize'

  fun ref add(entry: IndexEntry, node': (IndexNode| None) = None, index: USize = 0) ? =>
    let node: IndexNode = match node'
      | None => _root
      | let node: IndexNode => node
    end
    match node.bucket
      | None => // this is an internal Node
        if GetBit(entry.key, index + 1)? then
          add(entry, node.right, index + 1)?
        else
          add(entry, node.left, index + 1)?
        end
      | let bucket': List[IndexEntry] =>
        for entry' in bucket'.values() do
          if (entry.key == entry'.key) then //Update
            entry'.hits.increment() // TODO Should I even have and update!?
            return
          end
        end
        if (bucket'.size() < _bucketSize) then
          entry.hits.increment()
          bucket'.push(entry)
        else
          _split(node, index)?
          add(entry, node, index)?
        end
    end

    fun ref get(key: String val, node': (IndexNode | None) = None, index: USize = 0): (IndexEntry | None) ? =>
      let node : IndexNode = match node'
        | None => _root
        | let node: IndexNode => node
      end
      match node.bucket
        | None =>
          if GetBit(key, index + 1)? then
            get(key, node.right, index + 1)?
          else
            get(key, node.left, index + 1)?
          end
        | let bucket': List[IndexEntry] =>
          for entry' in bucket'.values() do
            if (key == entry'.key) then
              return entry'
            end
          end
          None
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
