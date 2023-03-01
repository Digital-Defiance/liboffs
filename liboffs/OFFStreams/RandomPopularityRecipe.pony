use "time"
use "random"
use "../BlockCache"
use "Streams"
use "Buffer"
use "Exception"
use "collections"

actor RandomPopularityRecipe[B: BlockType] is BlockRecipe[B]
  var _isDestroyed: Bool = false
  var _isReadable: Bool = true
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
    subscriberCount(DataEvent[Block[B]]) > 0

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
        _nextHash(_ranks as Array[U64])
      else
        _blockCache.ranks({(ranks: Array[U64] iso) (recipe: RandomPopularityRecipe[B] tag = this) =>  recipe._receiveRanks(consume ranks)} val)
      end
    end

  fun ref _nextHash(ranks': Array[U64] ) =>
    try
       _nextBlock(_currentRankHashes as Array[Buffer val])
    else
      try
        _blockCache.hashesAtRank(ranks'(_currentRankIndex)?, {(hashes: Array[Buffer val] iso) (recipe: RandomPopularityRecipe[B] tag = this) =>  recipe._receiveRankHashes(consume hashes)})
      else
        notifyComplete()
        _close()
      end
    end

  fun ref _nextBlock(currentRankHashes: Array[Buffer val]) =>
     let index: USize = gen.usize() % currentRankHashes.size()
     try
        let hash: Buffer val = currentRankHashes(index)?
        _blockCache.get(hash,{(block: (Block[B] | Exception | BlockNotFound)) (recipe: RandomPopularityRecipe[B] tag = this) =>
          recipe._receiveBlock(block)} val)
        currentRankHashes.remove(index, 1)
        if currentRankHashes.size() <= 0 then
          _currentRankHashes = None
          _currentRankIndex = _currentRankIndex - 1
          match _ranks
            | let ranks': Array[U64] =>
              _nextHash(ranks')
          end
        end
      else
        destroy(Exception("Array index out of bounds"))
      end


  be read(cb: {(Block[B])} val, size: (USize | None) = None) =>
    None

  be _receiveRanks(ranks: Array[U64] iso) =>
    var ranks': Array[U64] = consume ranks
    ranks' = Sort[Array[U64], U64](ranks')
    _currentRankIndex = ranks'.size() - 1
    let rankStr: String ref = String(ranks'.size())
    for i in ranks'.values() do
      if rankStr.size() > 0 then
        rankStr.append(",")
      end
      rankStr.append(i.string())
    end
    _ranks = ranks'
    if ranks'.size() <= 0 then
      notifyComplete()
      _close()
    else
      _nextHash(ranks')
    end

  be _receiveRankHashes(hashes: Array[Buffer val] iso) =>
    let currentRankHashes: Array[Buffer val] = consume hashes
    _currentRankHashes = currentRankHashes
    if currentRankHashes.size() <= 0 then
      _currentRankHashes = None
      _currentRankIndex = _currentRankIndex - 1
      match _ranks
        | let ranks': Array[U64] =>
          _nextHash(ranks')
      end
    else
      _nextBlock(currentRankHashes)
    end

  be _receiveBlock(block: (Block[B] | Exception | BlockNotFound)) =>
    match block
      | let block': Block[B] =>
        notifyData(block')
      | let err: Exception =>
        destroy(err)
      | BlockNotFound =>
        destroy(Exception("Block Not Found"))
    end

  be piped(stream: WriteablePullStream[Block[B]] tag) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let _stream: RandomPopularityRecipe[B] tag = this
        fun ref apply(ex: Exception) => _stream.destroy(ex)
      end
      stream.subscribe(consume errorNotify)
      let finishedNotify: FinishedNotify iso = object iso is FinishedNotify
        let _stream: RandomPopularityRecipe[B] tag = this
        fun ref apply() => _stream.close()
      end
      stream.subscribe(consume finishedNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: RandomPopularityRecipe[B] tag = this
        fun ref apply () => _stream.close()
      end
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
