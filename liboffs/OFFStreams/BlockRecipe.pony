use "Streams"
use "../BlockCache"
use "Buffer"

interface BlockRecipe[B: BlockType] is ReadablePullStream[Block[B] val]
