use "ponytest"
use "files"
use "../BlockCache"
class iso _TestBlockCache is UnitTest
  fun name(): String => "Testing Block Cache"
  fun apply(t: TestHelper) =>
    try
      let path: FilePath = FilePath(t.env.root as AmbientAuth, "offs/")?
      let bc = BlockCache[Standard](path)
    else
      t.fail("Creation Error")
      t.complete(true)
    end
