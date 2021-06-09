/*
use "Streams"
use "../BlockCache"
use "Buffer"

 actor WriteableOFFStream[B: BlockType] is WriteablePushStream[Buffer iso]
   let _accumulator: Array[U8]
   let _hasher: Hasher
   let _size: USize = 0
   let _blockCache: BlockCache[B]
   new create() =>
*/
