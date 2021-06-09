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
  var _block:(Block[B] | None) = None

  new create(blockCache: BlockCache[B]) =>
    _blockCache = blockCache
    _bs = BlockService[B]
    _subscribers' = Subscribers
    _isReadable = true

  fun readable(): Bool =>
    _isReadable

  fun _destroyed(): Bool =>
    _isDestroyed

  fun ref _subscribers() : Subscribers =>
    _subscribers'

  be pull() =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      try
        _block = _bs.newBlock()?
        _blockCache.put(_block,{(err:(None | SectionWriteError)) (recipe: RandomPopularity[B] tag = this) => recipe._releaseBlock(err)})
      else
        _notifyError(Exception("Failed to generate block"))
      end
    end

  be _releaseBlock(err: (None | SectionWriteError)) =>
    match err
      | SectionWriteError =>
        _notifyError(Exception("Failed to store block"))
    else
      try
        _notifyData(_block as Block[B])
        _block = None
      else
        _notifyError(Exception("Block Unavailable"))
      end
    end
  be read(cb: {(Block[B])} val, size: (USize | None) = None) =>
    None

  be piped(stream: WriteablePullStream[Array[U8] iso] tag) =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let _stream: ReadablePullStream[Array[U8] iso] tag = this
        fun ref apply(ex: Exception) => _stream.destroy(ex)
      end
      stream.subscribe(consume errorNotify)
      let finishedNotify: FinishedNotify iso = object iso is FinishedNotify
        let _stream: ReadablePullStream[Array[U8] iso] tag = this
        fun ref apply() => _stream.close()
      end
      stream.subscribe(consume finishedNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: ReadablePullStream[Array[U8] iso] tag = this
        fun ref apply () => _stream.close()
      end
      let closeNotify': CloseNotify tag = closeNotify
      stream.subscribe(consume closeNotify)
      _notifyPiped()
    end

  fun ref _close() =>
    if not _destroyed() then
      _isDestroyed = true
      _notifyClose()
      let subscribers: Subscribers = _subscribers()
      subscribers.clear()
    end

  be close() =>
    _close()
