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
    test(_TestFibonacciHitCounter)
    test(_TestFibonacciHitCounterJSON)
    test(_TestBlock)
    test(_TestBlockXOR)
    test(_TestIndex)
    test(_TestIndexJSON)
    test(_TestSection)
    test(_TestSections)
    test(_TestBlockCache)
    test(_TestNewBlocksRecipe)
