use "Base58"

class val Block
  let hash: Array[U8] val
  let data: Array[U8] val
  let key: String val

  new create(data': Array[U8] val) =>
    data = data'
    hash = SHA2Hash(data)
    key = recover val Base58.encode(hash) end

  new _withHash(data': Array[U8] val, hash': Array[U8] val, key: String val) =>
    data = data'
    hash = hash'
    key = key

  fun size()
    data.size()
