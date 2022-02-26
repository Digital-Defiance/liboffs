use "ponytest"
use "../BlockCache"
use "collections"
use "files"
use "json"

class iso _TestIndex is UnitTest
  fun name(): String => "Testing Index Creation"
  fun apply(t: TestHelper) =>
    try
      let bs: BlockService[Nano] = BlockService[Nano]
      let block1: Block[Nano] val = bs.newBlock()?
      let block2: Block[Nano] val = bs.newBlock()?
      let block3: Block[Nano] val = bs.newBlock()?
      let block4: Block[Nano] val = bs.newBlock()?

      let indexEntry1: IndexEntry = IndexEntry(block1.hash, 1, 0)
      let indexEntry2: IndexEntry = IndexEntry(block2.hash, 1, 1)
      let indexEntry3: IndexEntry = IndexEntry(block3.hash, 1, 2)
      let indexEntry4: IndexEntry = IndexEntry(block4.hash, 1, 3)
      let blockIndex : Index = Index(2, FilePath(t.env.root, "offs/blocks/nano/index/"))?
      blockIndex.add(indexEntry1)?
      blockIndex.add(indexEntry2)?
      blockIndex.add(indexEntry3)?
      blockIndex.add(indexEntry4)?
      t.assert_true(blockIndex.size() == 4)
      let list: List[IndexEntry] = blockIndex.list()
      t.assert_true(list.contains(indexEntry1))
      t.assert_true(list.contains(indexEntry2))
      t.assert_true(list.contains(indexEntry3))
      t.assert_true(list.contains(indexEntry4))
      let arr: Array[IndexEntry] = blockIndex.array()
      t.assert_true(arr.contains(indexEntry1))
      t.assert_true(arr.contains(indexEntry2))
      t.assert_true(arr.contains(indexEntry3))
      t.assert_true(arr.contains(indexEntry4))
      blockIndex.remove(indexEntry1.hash)?
      blockIndex.remove(indexEntry2.hash)?
      blockIndex.remove(indexEntry3.hash)?
      blockIndex.remove(indexEntry4.hash)?
      t.assert_true(blockIndex.size() == 0)
    else
      t.fail("Index Failed")
    end


class iso _TestIndexJSON is UnitTest
  fun name(): String => "Testing Index JSON"
    fun ref set_up(t: TestHelper) =>
      try
        let offDir = Directory(FilePath(t.env.root, "offs/"))?
        offDir.remove("nano")
      end
  fun apply(t: TestHelper) =>
    try
      let bs: BlockService[Nano] = BlockService[Nano]
      let block1: Block[Nano] val =  bs.newBlock()?
      let block2: Block[Nano] val =  bs.newBlock()?
      let block3: Block[Nano] val =  bs.newBlock()?
      let block4: Block[Nano] val =  bs.newBlock()?

      let path: FilePath = FilePath(t.env.root, "offs/blocks/nano/index/")

      let indexEntry1: IndexEntry = IndexEntry(block1.hash, 1, 0)
      let indexEntry2: IndexEntry = IndexEntry(block2.hash, 1, 1)
      let indexEntry3: IndexEntry = IndexEntry(block3.hash, 1, 2)
      let indexEntry4: IndexEntry = IndexEntry(block4.hash, 1, 3)
      let blockIndex : Index = Index(2, FilePath(t.env.root, "offs/blocks/nano/index/"))?
      blockIndex.add(indexEntry1)?
      blockIndex.add(indexEntry2)?
      blockIndex.add(indexEntry3)?
      blockIndex.add(indexEntry4)?

      let doc = JsonDoc
      doc.data = blockIndex.toJSON()
      let text: String val = doc.string()
      let doc2: JsonDoc val = recover val JsonDoc.>parse(text)? end
      let blockIndex2: Index = Index.fromJSON((doc2.data as JsonObject val), path)?
      t.assert_true(blockIndex2.size() == 4)
      let list: List[IndexEntry] = blockIndex2.list()
      t.assert_true(list.contains(indexEntry1))
      t.assert_true(list.contains(indexEntry2))
      t.assert_true(list.contains(indexEntry3))
      t.assert_true(list.contains(indexEntry4))
      blockIndex2.remove(indexEntry1.hash)?
      blockIndex2.remove(indexEntry2.hash)?
      blockIndex2.remove(indexEntry3.hash)?
      blockIndex2.remove(indexEntry4.hash)?
      t.assert_true(blockIndex2.size() == 0)
    else
      t.fail("Index Failed")
    end
