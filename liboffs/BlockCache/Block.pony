use "Base58"
use "time"
use "random"
use "collections"
use "Buffer"
use "Blake3"

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
  fun apply(size: USize, gen: Rand): Array[U8] =>
    var bytes: Array[U8] = Array[U8](size)
    for j in Range(0, size) do
      bytes.push(gen.u8())
    end
    bytes

class BlockService [B: BlockType]
  let gen: Rand
  new create () =>
    let now = Time.now()
    gen = Rand(now._1.u64(), now._2.u64())
  fun ref newBlock(data': Buffer val = recover val Buffer end): Block[B] ? =>
    let diff = BlockSize[B]() - data'.size()
    if (diff < 0) then
      error
    end
    let data = if (diff > 0) then
      let pad = RandomBytes(diff, gen)
      let full : Array[U8] iso  =  recover Array[U8](BlockSize[B]())  end
      for i in data'.values() do
        full.push(i)
      end
      for i in pad.values() do
        full.push(i)
      end
      recover Buffer(consume full) end
    else
      data'
    end
    Block[B](data)?


class val Block [B: BlockType]
  let hash: Buffer val
  let data: Buffer val

  new val create(data': Buffer val) ? =>
    if (data'.size() != BlockSize[B]()) then
      error
    end
    data = data'
    let hasher= Blake3(32)
    hasher.update(data.data)
    hash = Buffer.fromArray(hasher.digest())

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
