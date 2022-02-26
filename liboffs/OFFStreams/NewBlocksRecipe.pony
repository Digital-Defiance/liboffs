use "Streams"
use "../BlockCache"
use "Buffer"
use "Exception"

actor NewBlocksRecipe[B: BlockType] is BlockRecipe[B]
  var _isDestroyed: Bool = false
  var _isReadable: Bool = false
  let _blockCache: BlockCache[B]
  var _ranks: (Array[U64] | None) = None
  var _currentRankIndex: USize = 0
  var _currentRankHashes: (Array[Buffer val] | None) = None
  let _subscribers': Subscribers
  let _bs: BlockService[B]

  new create(blockCache: BlockCache[B]) =>
    _blockCache = blockCache
    _bs = BlockService[B]
    _subscribers' = Subscribers
    _isReadable = true

  fun readable(): Bool =>
    _isReadable

  fun destroyed(): Bool =>
    _isDestroyed

  fun ref subscribers() : Subscribers =>
    _subscribers'

  be pull() =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      try
        let block: Block[B] = _bs.newBlock()?
        _blockCache.put(block,{(err:(None | Exception)) (recipe: NewBlocksRecipe[B] tag = this, block': Block[B] = block) => recipe._releaseBlock(block', err)})
      else
        notifyError(Exception("Failed to generate block"))
      end
    end

  be _releaseBlock(block: Block[B], err: (None | Exception)) =>
    match err
    | let err': Exception =>
        notifyError(err')
    else
      notifyData(block)
    end

  be read(cb: {(Block[B])} val, size: (USize | None) = None) =>
    None

  be piped(stream: WriteablePullStream[Block[B]] tag) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let _stream: NewBlocksRecipe[B] tag = this
        fun ref apply(ex: Exception) => _stream.destroy(ex)
      end
      stream.subscribe(consume errorNotify)
      let finishedNotify: FinishedNotify iso = object iso is FinishedNotify
        let _stream: NewBlocksRecipe[B] tag = this
        fun ref apply() => _stream.close()
      end
      stream.subscribe(consume finishedNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: NewBlocksRecipe[B] tag = this
        fun ref apply () => _stream.close()
      end
      let closeNotify': CloseNotify tag = closeNotify
      stream.subscribe(consume closeNotify)
      notifyPiped()
    end

  fun ref _close() =>
    if not destroyed() then
      _isDestroyed = true
      notifyClose()
      let subscribers': Subscribers = subscribers()
      subscribers'.clear()
    end

  be close() =>
    _close()
