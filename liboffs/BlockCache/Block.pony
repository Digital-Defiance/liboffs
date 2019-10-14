use "Base58"
use "time"
use "random"
use "collections"
use "Buffer"

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

primitive RandomBytes
  fun apply(size: USize): Array[U8] =>
    let now = Time.now()
    var gen = Rand(now._1.u64(), now._2.u64())
    var bytes: Array[U8] = Array[U8](size)
    for j in Range(0, size) do
      bytes.push(gen.u8())
    end
    bytes

class val Block [B: BlockType]
  let hash: Buffer val
  let data: Buffer val

  new val create(data': Buffer val = recover val Buffer end) ? =>
    let diff = BlockSize[B]() - data'.size()
    if (diff < 0) then
      error
    end
    data = if (diff > 0) then
      recover
        let full : Array[U8] = Array[U8](BlockSize[B]())
        let pad = RandomBytes(diff)
        full.append(data'.data)
        full.append(pad)
        Buffer(full)
      end
    else
      data'
    end

    hash = Buffer.fromArray(SHA2Hash(data.data, 32))

  new val _withHash(data': Buffer val, hash': Buffer val)? =>
    if data'.size() != BlockSize[B]() then
      error
    end
    data = data'
    hash = hash'

  fun key(): String val ? =>
    recover val Base58.encode(hash.data)? end

  fun size(): USize =>
    data.size()

  fun box eq(that: box->Block[B]): Bool =>
    data == that.data

  fun box op_xor (that: box->Block[B]): Block[B] val ?=>
    let data2: Buffer val = recover
      let data': Array[U8] = Array[U8](BlockSize[B]())
      for i in Range(0, BlockSize[B]()) do
        data'.push(data(i)? xor that.data(i)?)
      end
      Buffer(data')
    end
    Block[B](data2)?
