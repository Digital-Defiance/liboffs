use "package:../BlockCache"
use "ponytest"
class iso _TestFibonacci is UnitTest
  fun name(): String => "Testing Fibonacci Sequence"
  fun apply(t: TestHelper) =>
    t.assert_true(Fibonacci(0) == 0)
    t.assert_true(Fibonacci(4) == 3)
    t.assert_true(Fibonacci(12) == 144)
    t.assert_true(Fibonacci(20) == 6765)
