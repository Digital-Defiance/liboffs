use "package:../BlockCache"
use "ponytest"
use "collections"

class iso _TestBlock is UnitTest
  fun name(): String => "Testing Block"
  fun apply(t: TestHelper) =>
    try
      let data: Array[U8] val = [1;2;3;4;5;6;7;8;9;10;11;12;13]
      let block: Block[Mega] val = Block[Mega](data)?
      t.assert_true(block.data.size() == BlockSize[Mega]())
      t.assert_true(block.key.size() > 0)
      t.assert_true(block.hash.size() == 34)
      t.assert_true(Block[Nano]()?.data.size() == BlockSize[Nano]())
    else
      t.fail("Block Creation Failed")
    end
