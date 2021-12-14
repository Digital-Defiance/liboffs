use "../BlockCache"
use "Streams"
use "Buffer"
use "Exception"
use "collections"

trait DescriptorNotify is Notify
  fun ref apply(descriptorHash: Buffer val)
  fun box hash(): USize =>
    50

primitive DescriptorKey is DescriptorNotify
  fun ref apply(descriptorHash: Buffer val) => None

actor WriteableDescriptor[B: BlockType] is WriteablePushStream[Array[Buffer val] val]
  let _bc: BlockCache[B]
  var _isDestroyed: Bool = false
  let _subscribers': Subscribers
  var _descriptor: Buffer ref
  let _tupleSize: USize
  let _cutPoint: USize
  let _blockSize: USize
  let _dataLength: USize
  let _blockCount: USize
  let _descBlockArr: Array[Block[B]] = Array[Block[B]]
  var _sentDescriptor: Bool = false
  var _descriptorPad: USize
  var _i: USize = 0

  new create(bc: BlockCache[B], descriptorPad: USize, tupleSize: USize, dataLength: USize) =>
    _subscribers' = Subscribers(3)
    _bc = bc
    _tupleSize = tupleSize
    _blockSize = BlockSize[B]()
    _descriptorPad = descriptorPad
    _descriptor = Buffer(Array[U8](_descriptorPad * _tupleSize))
    _cutPoint = ((_blockSize / _descriptorPad)  * _descriptorPad)
    _dataLength = dataLength
    _blockCount = _dataLength / _blockSize


  fun ref _subscriberCount[A: Notify](): USize =>
    let subscribers: Subscribers = _subscribers()
    try
      iftype A <: ThrottledNotify then
        subscribers(ThrottledKey)?.size()
      elseif A <: UnthrottledNotify then
        subscribers(ThrottledKey)?.size()
      elseif A <: ErrorNotify then
        subscribers(ErrorKey)?.size()
      elseif A <: PipedNotify then
        subscribers(PipedKey)?.size()
      elseif A <: UnpipedNotify then
        subscribers(UnpipedKey)?.size()
      elseif A <: DescriptorNotify  then
        subscribers(DescriptorKey)?.size()
      else
        0
      end
    else
      0
    end

  fun ref _subscribe(notify: Notify iso, once: Bool = false) =>
   if _destroyed() then
     _notifyError(Exception("Stream has been destroyed"))
   else
     let subscribers: Subscribers = _subscribers()
     let notify': Notify = consume notify
     try
       subscribers(notify')?.push((notify', once))
     else
       let arr: Subscriptions = Subscriptions(10)
       arr.push((notify', once))
       subscribers(notify') =  arr
     end
   end

 fun ref _notifyDescriptor(descriptorHash: Buffer val) =>
   try
     let subscribers: Subscribers = _subscribers()
     let onces = Array[USize](subscribers.size())
     var i: USize = 0
     for notify in subscribers(DescriptorKey)?.values() do
       match notify
       |  (let notify': DescriptorNotify, let once: Bool) =>
           notify'(descriptorHash)
           if once then
             onces.push(i)
           end
       end
       i = i + 1
     end
     if onces.size() > 0 then
       _discardOnces(subscribers(DescriptorKey)?, onces)
     end
     subscribers.clear()
     _sentDescriptor = true
   end



  fun ref _subscribers(): Subscribers=>
    _subscribers'

  fun _destroyed(): Bool =>
    _isDestroyed

  be write(data: Array[Buffer val] val) =>
    if data.size() != _tupleSize then
      destroy(Exception("Invalid Tuple"))
      return
    end
    for hash in data.values() do
      if hash.size() != _descriptorPad then
        destroy(Exception("Invalid Tuple"))
        return
      end
      _descriptor.append(hash)
    end

  be piped(stream: ReadablePushStream[Array[Buffer val] val] tag) =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      let dataNotify: DataNotify[Array[Buffer val] val] iso = object iso is DataNotify[Array[Buffer val] val]
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply(data': Array[Buffer val] val) =>
          _stream.write(consume data')
      end
      stream.subscribe(consume dataNotify)
      let errorNotify: ErrorNotify iso = object iso is ErrorNotify
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply(ex: Exception) => _stream.destroy(ex)
      end
      stream.subscribe(consume errorNotify)
      let completeNotify: CompleteNotify iso = object iso is CompleteNotify
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply() => _stream.close()
      end
      stream.subscribe(consume completeNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply () =>
          _stream.close()
      end
      let closeNotify': CloseNotify tag = closeNotify
      stream.subscribe(consume closeNotify)
      _notifyPiped()
    end

  be _putDescriptorBlocks(err: (None | SectionWriteError) = None) =>
    match err
    | let err': SectionWriteError =>
      destroy(Exception("Failed to store descriptor block"))
    else
      if _i < _descBlockArr.size() then
        try
          _bc.put(_descBlockArr(_i = _i + 1)?, {(err: (None | SectionWriteError)) (wd: WriteableDescriptor[B] tag = this) =>
            wd._putDescriptorBlocks(err)
           } val)
         else
           destroy(Exception("Failed to store descriptor block"))
        end
      else
        try
          _notifyDescriptor(_descBlockArr(0)?.hash)
          _close()
        else
          destroy(Exception("Failed to notify descriptor block"))
        end
      end
    end

  fun ref _buildDescriptorBlocks() =>
    let descBlockBytes: Array[Buffer] =  Array[Buffer]
    while _descriptor.size() > _cutPoint do
      let divider: USize = _cutPoint - _descriptorPad
      let des: Buffer = _descriptor.slice(0, divider)
      _descriptor.slice(divider, _descriptor.size())
      _descriptor = _descriptor.slice(divider, _descriptor.size())
      descBlockBytes.push(des)
    end
    if _descriptor.size() > 0 then
      descBlockBytes.push(_descriptor.slice())
    end
    try
      var prior: Buffer val = recover val Buffer end
      for i in Range[I64](descBlockBytes.size().i64(), 0, -1) do
        descBlockBytes(i.usize())?.append(prior)
        let descBlock = Block[B](descBlockBytes(i.usize())?.clone())?
        _descBlockArr.unshift(descBlock)
        prior = descBlock.hash
      end
      _putDescriptorBlocks()
    else
      destroy(Exception("Failure to create descriptor blocks"))
    end

  fun ref _close() =>
    if not _destroyed() then
      if _sentDescriptor then
        _notifyFinished()
        _isDestroyed = true
        _notifyClose()
        let subscribers: Subscribers = _subscribers()
        subscribers.clear()
      else
        _buildDescriptorBlocks()
      end
    end

  be close() =>
    _close()
