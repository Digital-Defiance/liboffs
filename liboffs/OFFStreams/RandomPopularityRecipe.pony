use "time"
use "random"
use "../BlockCache"
use "Streams"
use "Buffer"
use "Exception"

actor RandomPopularity[B: BlockType] is BlockRecipe[B]
  var _isDestroyed: Bool = false
  var _isReadable: Bool = false
  let _blockCache: BlockCache[B]
  var _ranks: (Array[U64] | None) = None
  var _currentRankIndex: USize = 0
  var _currentRankHashes: (Array[Buffer val] | None) = None
  let _subscribers': Subscribers
  let gen: Rand

  new create(blockCache: BlockCache[B]) =>
    _blockCache = blockCache
    _subscribers' = Subscribers
    let now = Time.now()
    gen = Rand(now._1.u64(), now._2.u64())

  fun ref _hasReaders(): Bool =>
    _subscriberCount[DataNotify[Block[B]]]() > 0

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
        _nextHash(_ranks as Array[U8])
      else
        _blockCache.ranks({(ranks: Array[U64] iso) (recipe: RandomPopularity[B] tag = this) =>  recipe._recieveRanks(consume ranks)} val)
      end
    end

  fun ref _nextHash(ranks': Array[U64] ) =>
    try
       _nextBlock(_currentRankHashes as Array[Buffer val])
    else
      _blockCache.hashesAtRank(try ranks'(_currentRankIndex)? else 0 end, {(hashes: Array[Buffer val] iso) (recipe: RandomPopularity[B] tag = this) =>  recipe._receiveRankHashes(consume hashes)})
    end

  fun ref _nextBlock(currentRankHashes': Array[Buffer val]) =>
   let index: USize = gen.usize() % _currentRankHashes.size()
   try
      let hash: Buffer val = _currentRankHashes(index)?
      _blockCache.get(hash,{(block: (Block[B] | SectionReadError | BlockNotFound)) (recipe: RandomPopularity[B] tag = this) => recipe._receiveBlock(block)} val)
      _currentRankHashes.remove(index)
      if _currentRankHashes.size() <= 0 then
        _currentRankHashes = None
        _currentRankIndex = _currentRankIndex + 1
        match _ranks
          | let ranks': Array[U64] =>
            if _currentRankIndex > _ranks.size() then
            _notifyComplete()
            end
          end
      end
    else
      destroy(Exception("Array index out of bounds"))
    end


  be read(cb: {(Block[B])} val, size: (USize | None) = None) =>
    None

  be _recieveRanks(ranks: Array[U64] iso) =>
    _ranks = consume ranks
    try _nextHash(_ranks as Array[U64]) end

  be _recieveRankHashes(hashes: Array[Buffer val] iso) =>
    _currentRankHashes = consume hashes
    try _nextBlock(_currentRankHashes as Array[Buffer val]) end

  be _receiveBlock(block: (Block[B] | SectionReadError | BlockNotFound)) =>
    match block
      | let block': Block[B] =>
        _notifyData(Block[B])
      | SectionReadError =>
        destroy(Exception("Section Read Error"))
      | BlockNotFound =>
        destroy(Exception("Block Not Found"))
    end

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
