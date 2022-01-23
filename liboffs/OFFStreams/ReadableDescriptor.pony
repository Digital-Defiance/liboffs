use "../BlockCache"
use "Streams"
use "Buffer"
use "Exception"
use "collections"

actor ReadableDescriptor[B: BlockType] is ReadablePushStream[Array[Buffer val] val]
  var _isDestroyed: Bool = false
  let _ori: ORI val
  let _blockSize: USize
  let _subscribers': Subscribers
  let _cutPoint: USize // maximum length of descriptor in bytes
  let _tupleCount: USize // total number of  tupples descriptor
  var _tupleCounter: USize = 0
  var _currentDescriptor: (Buffer | None) = None
  var _currentTuple: (Array[Buffer val] iso | None) = None
  let _bc: BlockCache[B]
  let _offsetTuple: USize // tuple containing offset
  var _offsetRemainder: Buffer
  let _descriptorPad: USize
  var _isReadable: Bool = true
  var _isPiped: Bool = false
  var _pipeNotifiers': (Array[Notify tag] iso | None) = None

  new create(bc: BlockCache[B], ori: ORI val, descriptorPad: USize) =>
    _subscribers' = Subscribers(3)
    _ori = ori
    _blockSize = BlockSize[B]()
    _tupleCount = (_ori.fileSize/ _blockSize) + (if (_ori.fileSize % _blockSize) > 0 then 1 else 0 end)
    _cutPoint = ((_blockSize / descriptorPad)  * descriptorPad)
    _offsetTuple = (_ori.fileOffset / _blockSize) + (if (_ori.fileSize % _blockSize) > 0 then 1 else 0 end)
    _descriptorPad = descriptorPad
    _bc = bc
    _offsetRemainder = Buffer(_ori.tupleSize * _descriptorPad)

  fun readable(): Bool =>
    _isReadable

  fun _destroyed(): Bool =>
    _isDestroyed

  fun ref _piped(): Bool =>
    _isPiped

  fun ref _pipeNotifiers(): (Array[Notify tag] iso^ | None) =>
    _pipeNotifiers' = None

  fun ref _subscribers() : Subscribers =>
    _subscribers'

  fun ref _autoPush(): Bool =>
    true

  be pipe(stream: WriteablePushStream[Array[Buffer val] val] tag) =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      let pipeNotifiers: Array[Notify tag] iso = try
         _pipeNotifiers() as Array[Notify tag] iso^
      else
        let pipeNotifiers' = recover Array[Notify tag] end
        consume pipeNotifiers'
      end

      let pipedNotify: PipedNotify iso =  object iso is PipedNotify
        let _stream: ReadablePushStream[Array[Buffer val] val] tag = this
        fun ref apply() =>
          _stream.push()
      end
      let pipedNotify': PipedNotify tag = pipedNotify
      pipeNotifiers.push(pipedNotify')
      stream.subscribe(consume pipedNotify)

      let errorNotify: ErrorNotify iso = object iso  is ErrorNotify
        let _stream: ReadablePushStream[Array[Buffer val] val] tag = this
        fun ref apply (ex: Exception) => _stream.destroy(ex)
      end
      let errorNotify': ErrorNotify tag = errorNotify
      pipeNotifiers.push(errorNotify')
      stream.subscribe(consume errorNotify)

      let closeNotify: CloseNotify iso = object iso  is CloseNotify
        let _stream: ReadablePushStream[Array[Buffer val] val] tag = this
        fun ref apply () => _stream.close()
      end
      let closeNotify': CloseNotify tag = closeNotify
      pipeNotifiers.push(closeNotify')
      stream.subscribe(consume closeNotify)

      _pipeNotifiers' = consume pipeNotifiers
      stream.piped(this)
      _isPiped = true
      _notifyPipe()
    end

  fun ref _moveToOffset(descriptor: Buffer): (Buffer val, Buffer) =>
    let descriptor': Buffer = if _offsetRemainder.size() > 0 then
      Buffer(descriptor.size() + _offsetRemainder.size()).>append(_offsetRemainder = Buffer(0)).>append(descriptor)
    else
      descriptor
    end
    if _offsetTuple > 0 then
      if _tupleCounter < _offsetTuple then
        _tupleCounter = (descriptor'.size() - _descriptorPad) / (_descriptorPad * _ori.tupleSize)
        let cut: USize = (descriptor'.size() - _descriptorPad) - ((descriptor'.size() - _descriptorPad) % (_descriptorPad * _ori.tupleSize))
        _offsetRemainder = descriptor'.slice(cut, descriptor'.size() - _descriptorPad)
        (CopyBufferRange(descriptor', (descriptor'.size() - _descriptorPad), descriptor'.size()), Buffer(0))
      else
        (CopyBufferRange(descriptor', 0, _descriptorPad), descriptor'.slice(_descriptorPad))
      end
    else
      (CopyBufferRange(descriptor', 0, _descriptorPad), descriptor'.slice(_descriptorPad))
    end

  be _receiveDescriptorBlock(block: (Block[B] | SectionReadError | BlockNotFound), cb: ({(Array[Buffer val] val)} val | None) = None) =>
    match block
      | SectionReadError => destroy(Exception("Section Read Error"))
      | BlockNotFound =>  destroy(Exception("Descriptor Block Not Found"))
      | let block': Block[B] =>
        var currentDescriptor: Buffer = match _currentDescriptor
          | None =>
            var currentDescriptor': Buffer = block'.data.slice(0, _cutPoint)
            currentDescriptor' = block'.data.slice(0, _cutPoint)
            if block'.hash == _ori.descriptorHash then
              currentDescriptor' = currentDescriptor'.slice(_ori.descriptorOffset)
            end
            _currentDescriptor = currentDescriptor'
            currentDescriptor'
          | let currentDescriptor: Buffer =>
            currentDescriptor
        end
        while currentDescriptor.size() > 0  do
          var currentTuple: Array[Buffer val] iso = try
            ((_currentTuple = None) as Array[Buffer val] iso^ )
          else
            recover Array[Buffer val](_ori.tupleSize) end
          end

          var cursor: (Buffer val, Buffer) = _moveToOffset(currentDescriptor)
          var key: Buffer val = cursor._1
          currentDescriptor = cursor._2
          while currentTuple.size() < _ori.tupleSize do
            if currentDescriptor.size() <= 0 then
              _getDescriptor(key)
              _currentTuple = consume currentTuple
              _currentDescriptor = None
              return
            else
              currentTuple.push(key)
              if currentTuple.size() == _ori.tupleSize then
                break
              else
                cursor = _moveToOffset(currentDescriptor)
                key = cursor._1
                currentDescriptor = cursor._2
              end
            end
          end
          match cb
            | None =>
              _notifyData(consume currentTuple)
            | let cb': {(Array[Buffer val] val)} val =>
              cb'(consume currentTuple)
          end
          _tupleCounter = _tupleCounter + 1
          if _tupleCounter >= _tupleCount then
            _notifyComplete()
            close()
            return
          end
        end
    end

  be _getDescriptor(key: Buffer val) =>
    let cb' =  {(block: (Block[B] | SectionReadError | BlockNotFound)) (rd: ReadableDescriptor[B] tag = this) =>
      rd._receiveDescriptorBlock(block)
    } val
    _bc.get(_ori.descriptorHash, cb')

  be push() =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      match _currentDescriptor
        | None =>
          let cb' =  {(block: (Block[B] | SectionReadError | BlockNotFound)) (rd: ReadableDescriptor[B] tag = this) =>
            rd._receiveDescriptorBlock(block)
          } val
          _bc.get(_ori.descriptorHash, cb')
      end
    end

  be read(cb: {(Array[Buffer val] val)} val, size:(USize | None) = None) =>
    if _destroyed() then
      _notifyError(Exception("Stream has been destroyed"))
    else
      match _currentDescriptor
        | None =>
          let cb' =  {(block: (Block[B] | SectionReadError | BlockNotFound)) (rd: ReadableDescriptor[B] tag = this) =>
            rd._receiveDescriptorBlock(block, cb)
          } val
          _bc.get(_ori.descriptorHash, cb')
      end
    end

  be destroy(message: (String | Exception)) =>
    if not _destroyed() then
      match message
        | let message' : String =>
          _notifyError(Exception(message'))
        | let message' : Exception =>
          _notifyError(message')
      end
      _isDestroyed = true
      let subscribers: Subscribers = _subscribers()
      subscribers.clear()
    end
  be close() =>
    if not _destroyed() then
      _isDestroyed = true
      _notifyClose()
      let subscribers: Subscribers = _subscribers()
      subscribers.clear()
      _pipeNotifiers' = None
      _isPiped = false
    end
