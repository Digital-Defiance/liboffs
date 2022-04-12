use "Streams"
use "Exception"
use "../BlockCache"
use "Buffer"
use "Base58"


actor ReadableOffStream[B: BlockType] is TransformPushStream[Buffer iso, Tuple val]
  var _readable: Bool = false
  var _isDestroyed: Bool = false
  let _subscribers': Subscribers
  var _pipeNotifiers': (Array[Notify tag] iso | None) = None
  var _isPiped: Bool = false
  let _ori: ORI val
  var _currentDescriptor: (Buffer | None) = None
  var _tuples: (Array[(Tuple val, Array[Block[B]])] | None) = None
  var _offsetRemainder: USize =0
  let _bc: BlockCache[B]
  let _tc: TupleCache
  let _descriptorPad: USize
  var _sentBytes: USize = 0
  var _firstTuple: (Tuple val | None) = None
  var _currentTupleIndex: USize = 0

  new create(bc: BlockCache[B], tc: TupleCache, ori: ORI val, descriptorPad: USize) =>
    _subscribers' = Subscribers(3)
    _bc = bc
    _tc = tc
    _ori = ori
    _descriptorPad = descriptorPad
    let blockSize = BlockSize[B]()
    _sentBytes = (_ori.fileOffset / blockSize) * blockSize
    _offsetRemainder = _ori.fileOffset % blockSize

  fun ref subscribers(): Subscribers =>
    _subscribers'

  fun destroyed(): Bool =>
    _isDestroyed

  fun readable(): Bool =>
    _readable

  fun ref isPiped(): Bool =>
    _isPiped

  fun _isFirstTuple(tuple: Tuple val) : Bool =>
    match _firstTuple
      | None => false
      | let firstTuple: Tuple val =>
        firstTuple is tuple
    end

  fun ref pipeNotifiers(): (Array[Notify tag] iso^ | None) =>
    _pipeNotifiers' = None

  be piped(stream: ReadablePushStream[Tuple val] tag) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      let dataNotify: DataNotify[Tuple val] iso = object iso is DataNotify[Tuple val]
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply(data': Tuple val) =>
          _stream.write(data')
      end
      stream.subscribe(consume dataNotify)
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply(ex: Exception) => _stream.destroy(ex)
      end
      stream.subscribe(consume errorNotify)
      let completeNotify: CompleteNotify iso = object iso is CompleteNotify
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply() =>
          None
      end
      stream.subscribe(consume completeNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply () => None
      end
      let closeNotify': CloseNotify tag = closeNotify
      stream.subscribe(consume closeNotify)
      if isPiped() then
        notifyPiped()
      end
    end

  be pipe(stream: WriteablePushStream[Buffer iso] tag) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      let pipeNotifiers': Array[Notify tag] iso = try
         pipeNotifiers() as Array[Notify tag] iso^
      else
        let pipeNotifiers'' = recover Array[Notify tag] end
        consume pipeNotifiers''
      end

      let pipedNotify: PipedNotify iso = object iso  is PipedNotify
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply () => _stream._endPiped()
      end
      let pipedNotify': PipedNotify tag = pipedNotify
      stream.subscribe(consume pipedNotify)
      pipeNotifiers'.push(pipedNotify')

      let errorNotify: ErrorNotify iso = object iso  is ErrorNotify
        let _stream: ReadableOffStream[B] tag = this
        fun ref apply (ex: Exception) => _stream.destroy(ex)
      end
      let errorNotify': ErrorNotify tag = errorNotify
      pipeNotifiers'.push(errorNotify')
      stream.subscribe(consume errorNotify)
      _pipeNotifiers' = consume pipeNotifiers'
      stream.piped(this)
      notifyPipe()
    end

  be _endPiped() =>
    _isPiped = true
    notifyPiped()

  be push() =>
    None

  be read(cb: {(Buffer iso)} val, size: (USize | None) = None) =>
    None

  fun ref _renderOriginData(data: (Buffer val | None) = None) =>
    match _tuples
      | let tuples: Array[(Tuple val, Array[Block[B]])] =>
        match data
        | let data': Buffer val =>
          try
            let tuple: (Tuple val, Array[Block[B]]) = tuples.shift()?
            _currentTupleIndex = 0
            let range: (USize, USize) = if _isFirstTuple(tuple._1) then
              if ((_sentBytes + (data'.size() - _offsetRemainder)) > _ori.finalByte) then
                (_offsetRemainder, _ori.finalByte - _sentBytes)
              else
                (_offsetRemainder, data'.size())
              end
            else
              if ((_sentBytes + data'.size()) > _ori.finalByte) then
                (0, _ori.finalByte - _sentBytes)
              else
                (0, data'.size())
              end
            end
            notifyData(CopyBufferRange(data', range._1, range._2))
            _sentBytes = _sentBytes + (range._2 - range._1)
            if (_sentBytes >= _ori.finalByte) then
              _firstTuple = None
              notifyFinished()
              notifyComplete()
              _close()
            else
              _currentTupleIndex = 0
              _checkCache()
            end
          else
            destroy(Exception("Failed to retrieve original data"))
          end
        | None =>
          try
            let currentTuple = tuples.shift()?
            _currentTupleIndex = 0
            var originData: Buffer val = currentTuple._2.shift()?.data
            for block in currentTuple._2.values() do
              originData =  recover val originData xor block.data end
            end
            let originBlock: Block[B] = Block[B](originData)?
            _tc(currentTuple._1) = originBlock.data
            let range: (USize, USize) = if _isFirstTuple(currentTuple._1) then
              if ((_sentBytes + (originBlock.data.size() - _offsetRemainder)) > _ori.finalByte) then
                (_offsetRemainder, _ori.finalByte - _sentBytes)
              else
                (_offsetRemainder, originBlock.data.size())
              end
            else
              if ((_sentBytes + originBlock.data.size()) > _ori.finalByte) then
                (0, _ori.finalByte - _sentBytes)
              else
                (0, originBlock.data.size())
              end
            end
            notifyData(CopyBufferRange(originBlock.data, range._1, range._2))
            _sentBytes = _sentBytes + (range._2 - range._1)
            if _sentBytes >= _ori.finalByte then
              notifyFinished()
              notifyComplete()
              _close()
            else
              _currentTupleIndex = 0
              _checkCache()
            end
          else
            destroy(Exception("Failed to retrieve original data"))
          end
        end
    else
      destroy(Exception("Failed to retrieve original data"))
    end
  be _receiveCacheHit(data: Buffer val) =>
    _renderOriginData(consume data)

  be _receiveCacheMiss() =>
    _getTupleBlocks()

  be _receiveTupleBlocks(block: (Block[B] | Exception | BlockNotFound)) =>
    match block
      | let block': Block[B] =>
        match _tuples
        | let tuples: Array[(Tuple val, Array[Block[B]])] =>
            try
              let currentTuple = tuples(0)?
              currentTuple._2.push(block')
              if currentTuple._2.size() >= currentTuple._1.size() then
                _renderOriginData()
              else
                _getTupleBlocks()
              end
            end
        end
      | let ex: Exception =>
        destroy(ex)
      | BlockNotFound =>
        destroy(Exception("Block Not Found - " + try Base58.encode((_tuples as Array[(Tuple val, Array[Block[B]])])(0)?._1(_currentTupleIndex - 1)?.data)? else "" end))
    end

  fun ref _checkCache() =>
    match _tuples
    | let tuples: Array[(Tuple val, Array[Block[B]])] =>
        if tuples.size() < 1 then
          return
        end
        try
          let currentTuple = tuples(0)?
          if currentTuple._2.size() == 0 then
            _tc(currentTuple._1, {(data: (Buffer val | None)) (stream: ReadableOffStream[B] tag = this) =>
              match data
                | None =>
                  stream._receiveCacheMiss()
                | let data': Buffer val =>
                  stream._receiveCacheHit(data')
              end
            })
          else
            let cb = {(block: (Block[B] | Exception | BlockNotFound)) (stream: ReadableOffStream[B] tag = this) =>
              stream._receiveTupleBlocks(block)
            } val
            _bc.get(currentTuple._1(_currentTupleIndex = _currentTupleIndex + 1)?, cb)
          end
        end
      else
        destroy(Exception("Invalid Tuples Data"))
    end

  fun ref _getTupleBlocks() =>
    match _tuples
      | let tuples: Array[(Tuple val, Array[Block[B]])] =>
        try
          let currentTuple = tuples(0)?
          let cb = {(block: (Block[B] | Exception | BlockNotFound)) (stream: ReadableOffStream[B] tag = this) =>
            stream._receiveTupleBlocks(block)
          } val
          _bc.get(currentTuple._1(_currentTupleIndex = _currentTupleIndex + 1)?, cb)
        else
          destroy(Exception("Error Retrieving Tuple Blocks"))
        end
    else
      destroy(Exception("Error Retrieving Tuple Blocks"))
    end

  be write(data: Tuple val) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      match _tuples
        | None =>
          _firstTuple = data
          let tuples = Array[(Tuple val, Array[Block[B]])](30)
          tuples.push((data , Array[Block[B]](data.size())))
          _tuples = tuples
          if tuples.size() < 2 then
            _checkCache()
          end
        | let tuples: Array[(Tuple val, Array[Block[B]])] =>
          tuples.push((data , Array[Block[B]](data.size())))
          if tuples.size() < 2 then
            _checkCache()
          end
      end
    end

  fun ref _close() =>
    if not destroyed() then
      _isDestroyed = true
      notifyClose()
      let subscribers': Subscribers = subscribers()
      subscribers'.clear()
      _pipeNotifiers' = None
    end

  be close() =>
    _close()
