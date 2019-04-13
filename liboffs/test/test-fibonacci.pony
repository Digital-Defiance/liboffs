use "package:../BlockCache"
use "ponytest"
use "collections"

class iso _TestFibonacci is UnitTest
  fun name(): String => "Testing Fibonacci Sequence"
  fun apply(t: TestHelper) =>
    t.assert_true(Fibonacci(0) == 0)
    t.assert_true(Fibonacci(4) == 3)
    t.assert_true(Fibonacci(12) == 144)
    t.assert_true(Fibonacci(20) == 6765)

class iso _TestFibonacciHitCounter is UnitTest
  fun name(): String => "Testing Fibonacci Hit Counter"
  fun apply(t: TestHelper) =>
    let counter1 : FibonacciHitCounter = FibonacciHitCounter
    let counter2 : FibonacciHitCounter = FibonacciHitCounter
    for i in Range(0, 240) do
      if (i < 50) then
        counter1.increment()
      end
      counter2.increment()
    end
    t.assert_true(counter1 < counter2)
    t.assert_true(counter2 > counter1)
    t.assert_false(counter2 == counter1)
    t.assert_true(counter2 != counter1)
    for i in Range(0, 240 - 50) do
      counter1.increment()
    end
    t.assert_true(counter2 == counter1)
    t.assert_false(counter1 < counter2)
    t.assert_false(counter2 > counter1)
    t.assert_false(counter2 != counter1)
