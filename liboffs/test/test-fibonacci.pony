use "package:../BlockCache"
use "pony_test"
use "collections"
use "json"

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
    let fibInc = {(num: U64, counter: FibonacciHitCounter): Bool =>
      var result: Bool = false
      for i in Range(0, Fibonacci(num).usize() + 1) do
        result = counter.increment()
      end
      result
    } val
    for i in Range(0, 10) do
      t.assert_true(fibInc(i.u64(), counter1))
    end
    for i in Range(0, 20) do
      t.assert_true(fibInc(i.u64(), counter2))
    end

    t.assert_true(counter1 < counter2)
    t.assert_true(counter2 > counter1)
    t.assert_false(counter2 == counter1)
    t.assert_true(counter2 != counter1)

class iso _TestFibonacciHitCounterJSON is UnitTest
  fun name(): String => "Testing Fibonacci Hit Counter JSON"
  fun apply(t: TestHelper) =>
    let counter1 : FibonacciHitCounter = FibonacciHitCounter
    let counter2 : FibonacciHitCounter = FibonacciHitCounter
    for i in Range(0, 240) do
      if (i < 50) then
        counter1.increment()
      end
      counter2.increment()
    end
    try
      let doc1 = JsonDoc
      doc1.data = counter1.toJSON()
      let doc3: JsonDoc val = recover val JsonDoc.>parse(doc1.string())? end

      let doc2 = JsonDoc
      doc2.data = counter2.toJSON()
      let doc4: JsonDoc val = recover val JsonDoc.>parse(doc2.string())? end

      let counter3 : FibonacciHitCounter = FibonacciHitCounter.fromJSON(doc3.data as JsonObject val)?
      let counter4 : FibonacciHitCounter = FibonacciHitCounter.fromJSON(doc4.data as JsonObject val)?

      t.assert_true(counter1 == counter3)
      t.assert_true(counter2 == counter4)
    else
      t.fail("Parse Error")
      t.complete(true)
    end
