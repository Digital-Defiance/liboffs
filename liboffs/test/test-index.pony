use "ponytest"
use "../BlockCache"
use "collections"
use "files"

class iso _TestIndex is UnitTest
  fun name(): String => "Testing Index Creation"
  fun apply(t: TestHelper) =>
    try
      let block1: Block[Nano] val = Block[Nano]()?
      let block2: Block[Nano] val = Block[Nano]()?
      let block3: Block[Nano] val = Block[Nano]()?
      let block4: Block[Nano] val = Block[Nano]()?

      let path: FilePath = FilePath(t.env.root as AmbientAuth, "./sections")?

      let indexEntry1: IndexEntry = IndexEntry(block1.key, 1, 0)
      let indexEntry2: IndexEntry = IndexEntry(block2.key, 1, 1)
      let indexEntry3: IndexEntry = IndexEntry(block3.key, 1, 2)
      let indexEntry4: IndexEntry = IndexEntry(block4.key, 1, 3)
      let blockIndex : Index = Index(2)
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
      blockIndex.remove(indexEntry1.key)?
      blockIndex.remove(indexEntry2.key)?
      blockIndex.remove(indexEntry3.key)?
      blockIndex.remove(indexEntry4.key)?
      t.assert_true(blockIndex.size() == 0)
    else
      t.fail("Index Failed")
    end
