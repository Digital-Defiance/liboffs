use "package:../BlockCache"
use "ponytest"

class iso _TestSHA2 is UnitTest
  fun name(): String => "Testing SHA256 34 Bit Digest"
  fun apply(t: TestHelper) =>
    let data : Array[U8] val = [1;2;3;4;5;6;7;8;9;10]
    let digest: Array[U8] val = SHA2Hash(data, 34)
    t.assert_true(digest.size() == 34)
