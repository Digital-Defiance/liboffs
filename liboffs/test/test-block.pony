use "package:../BlockCache"
use "ponytest"
use "collections"
use "Buffer"

class iso _TestBlock is UnitTest
  fun name(): String => "Testing Block Creation"
  fun apply(t: TestHelper) =>
    try
      let data: Buffer val = recover Buffer.fromArray([1;2;3;4;5;6;7;8;9;10;11;12;13]) end
      let block: Block[Mega] val = Block[Mega](data)?
      t.assert_true(block.data.size() == BlockSize[Mega]())
      t.assert_true(block.key()?.size() > 0)
      t.assert_true(block.hash.size() == 32)
      t.assert_true(Block[Nano]()?.data.size() == BlockSize[Nano]())
    else
      t.fail("Block Creation Failed")
    end

class iso _TestBlockXOR is UnitTest
  fun name(): String => "Testing Block XOR"
  fun apply(t: TestHelper) =>
    try
      let data: Buffer val = recover Buffer.fromArray([1;2;3;4;5;6;7;8;9;10;11;12;13]) end
      let block1: Block[Standard] val = Block[Standard](data)?
      let block2: Block[Standard] val = Block[Standard]()?
      let block3: Block[Standard] val = block1 xor? block2
      let block4: Block[Standard] val = block2 xor? block3
      t.assert_array_eq[U8](block1.data, block4.data)
      t.assert_true(block3.data.size() == BlockSize[Standard]())
    else
      t.fail("Block XOR Failed")
    end
