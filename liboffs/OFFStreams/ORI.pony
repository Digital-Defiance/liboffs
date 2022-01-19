use "Buffer"
use "../BlockCache"
use "../Global"
class ORI
  var descriptorHash: Buffer val
  var descriptorOffset: USize
  var blockType: BlockType
  var tupleSize: USize
  var fileHash: Buffer val
  var fileOffset: USize
  var fileName: String
  var fileSize: USize
  new create(fileName': String = "", descriptorHash': Buffer val = recover val Buffer end, descriptorOffset': USize = 0, blockType': BlockType = Standard, tupleSize': USize = 0, fileOffset': USize = 0, fileHash': Buffer val = recover val Buffer end, fileSize': USize) =>
    descriptorHash = descriptorHash'
    descriptorOffset = descriptorOffset'
    blockType = blockType'
    tupleSize = tupleSize'
    fileHash = fileHash'
    fileOffset = fileOffset'
    fileName = fileName'
    fileSize = fileSize'
