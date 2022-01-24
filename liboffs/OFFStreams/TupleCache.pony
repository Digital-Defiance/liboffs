use "LRUCache"
use "Buffer"
actor TupleCache
  let _cache: LRUCache[Tuple val, Buffer val]
  new create(size': USize) =>
    _cache = LRUCache[Tuple val, Buffer val](size')
be apply(key: Tuple val, cb: {((Buffer val | None))} val) =>
  cb(_cache(key))

be update(key: Tuple val, value: Buffer val) =>
  _cache(key) = value

be remove(key: Tuple val) =>
  _cache.remove(key)

be contains(key: Tuple val, cb: {(Bool)} val) =>
  cb(_cache.contains(key))

be size(cb: {(USize)} val) =>
  cb(_cache.size())

be capacity(cb: {(USize)} val) =>
  cb(_cache.capacity())
