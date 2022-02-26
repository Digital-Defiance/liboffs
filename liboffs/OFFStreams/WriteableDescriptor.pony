use "../BlockCache"
use "Streams"
use "Buffer"
use "Exception"
use "collections"

trait DescriptorHashNotify is Notify
  fun ref apply(descriptorHash: Buffer val)
  fun box hash(): USize =>
    50

primitive DescriptorKey is DescriptorHashNotify
  fun ref apply(descriptorHash: Buffer val) => None

actor WriteableDescriptor[B: BlockType] is WriteablePushStream[Tuple val]
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
  var _bs: BlockService[B]

  new create(bc: BlockCache[B], descriptorPad: USize, tupleSize: USize, dataLength: USize, bs: BlockService[B] iso = recover BlockService[B] end) =>
    _subscribers' = Subscribers(3)
    _bc = bc
    _tupleSize = tupleSize
    _blockSize = BlockSize[B]()
    _descriptorPad = descriptorPad
    _descriptor = Buffer(Array[U8](_descriptorPad * _tupleSize))
    _cutPoint = ((_blockSize / _descriptorPad)  * _descriptorPad)
    _dataLength = dataLength
    _blockCount = _dataLength / _blockSize
    _bs = consume bs


  fun ref subscriberCount[A: Notify](): USize =>
    let subscribers': Subscribers = subscribers()
    try
      iftype A <: ThrottledNotify then
        subscribers'(ThrottledKey)?.size()
      elseif A <: UnthrottledNotify then
        subscribers'(ThrottledKey)?.size()
      elseif A <: ErrorNotify then
        subscribers'(ErrorKey)?.size()
      elseif A <: PipedNotify then
        subscribers'(PipedKey)?.size()
      elseif A <: UnpipedNotify then
        subscribers'(UnpipedKey)?.size()
      elseif A <: DescriptorHashNotify  then
        subscribers'(DescriptorKey)?.size()
      else
        0
      end
    else
      0
    end

 fun ref notifyDescriptor(descriptorHash: Buffer val) =>
   try
     let subscribers': Subscribers = subscribers()
     let onces = Array[USize](subscribers'.size())
     var i: USize = 0
     for notify in subscribers'(DescriptorKey)?.values() do
       match notify
       |  (let notify': DescriptorHashNotify, let once: Bool) =>
           notify'(descriptorHash)
           if once then
             onces.push(i)
           end
       end
       i = i + 1
     end
     if onces.size() > 0 then
       discardOnces(subscribers'(DescriptorKey)?, onces)
     end
     _sentDescriptor = true
   end



  fun ref subscribers(): Subscribers=>
    _subscribers'

  fun destroyed(): Bool =>
    _isDestroyed

  be write(data: Tuple val) =>
    if data.size() != _tupleSize then
      destroy(Exception("Invalid Tuple Size " + data.size().string()))
      return
    end
    for hash in data.values() do
      if hash.size() != _descriptorPad then
        destroy(Exception("Invalid Tuple Hash Size"))
        return
      end
      _descriptor.append(hash)
    end

  be piped(stream: ReadablePushStream[Tuple val] tag) =>
    if destroyed() then
      notifyError(Exception("Stream has been destroyed"))
    else
      let dataNotify: DataNotify[Tuple val] iso = object iso is DataNotify[Tuple val]
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply(data': Tuple val) =>
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
        fun ref apply() =>
          _stream.close()
      end
      stream.subscribe(consume completeNotify)
      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: WriteableDescriptor[B] tag = this
        fun ref apply () =>
          _stream.close()
      end
      let closeNotify': CloseNotify tag = closeNotify
      stream.subscribe(consume closeNotify)
      notifyPiped()
    end

  be _putDescriptorBlocks(err: (None | Exception) = None) =>
    match err
    | let err': Exception =>
      destroy(Exception("Failed to store descriptor block"))
    else
      if _i < _descBlockArr.size() then
        try
          _bc.put(_descBlockArr(_i = _i + 1)?, {(err: (None | Exception)) (wd: WriteableDescriptor[B] tag = this) =>
            wd._putDescriptorBlocks(err)
           } val)
         else
           destroy(Exception("Failed to store descriptor block"))
        end
      else
        try
          notifyDescriptor(_descBlockArr(0)?.hash)
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
    var prior: Buffer val = recover val Buffer end
    for i in Range[I64](descBlockBytes.size().i64() -1, -1, -1) do
      try
        descBlockBytes(i.usize())?.append(prior)
      else
        destroy(Exception("Failure to append descriptor data"))
        return
      end
      try
        let descBlock = _bs.newBlock(descBlockBytes(i.usize())?.clone())?
        _descBlockArr.unshift(descBlock)
        prior = descBlock.hash
      else
          destroy(Exception("Failure to create descriptor block"))
          return
      end
    end
    _putDescriptorBlocks()


  fun ref _close() =>
    if not destroyed() then
      if _sentDescriptor then
        notifyFinished()
        _isDestroyed = true
        notifyClose()
        let subscribers': Subscribers = subscribers()
        subscribers'.clear()
      else
        _buildDescriptorBlocks()
      end
    end

  be close() =>
    _close()
