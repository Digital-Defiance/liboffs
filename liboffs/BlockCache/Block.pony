use "Base58"

primitive Mega
primitive Nano
primitive Standard
primitive Mini

type BlockType is (Mega | Standard | Mini | Nano)

primitive BlockSize
  fun apply[B: BlockType](): USize =>
      iftype B <: Mega then
        1000000
      elseif B <: Standard then
        128000
      elseif B <: Mini then
         1000
      elseif B <: Nano then
        136
      else
        0// TODO Maybe this should throw an error
      end
class val Block [B: BlockType]
  let hash: Array[U8] val
  let data: Array[U8] val
  let key: String val

  new create(data': Array[U8] val)? =>
    if (data'.size() > BlockSize[B]()) then
      error
    end
    data = data'
    hash = SHA2Hash(data)
    key = recover val Base58.encode(hash)? end

  new _withHash(data': Array[U8] val, hash': Array[U8] val, key': String val) =>
    data = data'
    hash = hash'
    key = key'

  fun size(): USize =>
    data.size()
