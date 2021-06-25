use "Streams"
use "../BlockCache"
use "Buffer"
use "Exception"

interface BlockRecipe[B: BlockType] is ReadablePullStream[Block[B] val]
