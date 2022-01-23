use "Buffer"

class Tuple
  let hashes: Array[Buffer val]
  let _size: USize
  new create(size': USize = 3) =>
    hashes = Array[Buffer val](size')
    _size = size'


  fun apply(i: USize) : Buffer val ? =>
    hashes(i)?

  fun hash(): USize =>
    var hash' : USize = 5381
    for hash'' in hashes.values() do
      for num in hash''.values() do
        hash' = (((hash' << 5) >> 0) + hash') + num.usize()
      end
    end
    hash'

  fun hash64(): U64 =>
    var hash' : U64 = 5381
    for hash'' in hashes.values() do
      for num in hash''.values() do
        hash' = (((hash' << 5) >> 0) + hash') + num.u64()
      end
    end
    hash'

  fun ref update(i: USize, value: Buffer val): Buffer val^ ? =>
    hashes(i)? = value

  fun ref push(value: Buffer val) ? =>
    if hashes.size() < _size then
      hashes.push(value)
    else
      error
    end

  fun ref pop(): Buffer val^ ? =>
    hashes.pop()?

  fun ref unshift(value: Buffer val) ? =>
    if hashes.size() < _size then
      hashes.unshift(value)
    else
      error
    end

  fun ref shift(): Buffer val ? =>
    hashes.shift()?


  fun box values() : ArrayValues[Buffer val, this->Array[Buffer val]]^ =>
    hashes.values()

  fun box size(): USize =>
    hashes.size()
