use "ponytest"
use "../BlockCache"
use "collections"

actor Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)
  new make () =>
    None
  fun tag tests(test: PonyTest) =>
    test(_TestFibonacci)
    test(_TestSHA2)
