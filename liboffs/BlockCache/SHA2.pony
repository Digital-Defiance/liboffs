use "path:/usr/local/opt/libressl/lib" if osx
use "lib:crypto"
use "crypto"

primitive SHA2Hash
  fun tag apply(input: ByteSeq, size: USize = 32): Array[U8] val =>
    recover
      let digest =
        @pony_alloc[Pointer[U8]](@pony_ctx[Pointer[None] iso](), size)
      @SHA256[Pointer[U8]](input.cpointer(), input.size(), digest)
      Array[U8].from_cpointer(digest, size)
    end
