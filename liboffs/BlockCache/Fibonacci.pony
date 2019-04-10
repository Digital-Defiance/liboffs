primitive Fibonacci
  apply(num: U64) : U64 =>
    var a: U64 = 1
    var b: U64 = 0
    var temp: U64 = 0
    var start: U64 = num.u64()
    while (start >= 0) do
      temp = a
      a = a + b
      b = temp
      start = start - 1
    end
    b
class FibonacciHitCounter
  let _fib : U64 = 0
  let _count: U64 = 0
  let _threshold: U64
  new create() =>
    _threshold = Fibonacci(_fib)
  new from(fib': U64, count': U64)
    _fib = fib'
    _count = count'
    _threshold = Fibonacci(_fib)
  fun ref increment() =>
    _count = _count + 1
    if (_count >= _threshold) then
      _fib = _fib + 1
      _count = 0
      _threshold(
    end
  fun ref decrement() =>
    _threshold = Fibonacci(_fib)

  fun box eq(that: box->FibonacciHitCounter): Bool >
    if ((that._fab == _fab) and (that._count == _count)) then
      true
    else
      false
    end
  fun box ne(that: box->FibonacciHitCounter): Bool =>
    not eq(that)

  fun box gt (that: box->FibonacciHitCounter): Bool =>
    if (_fib < that._fib) then
      return false
    else if (_fib > that._fib) then
      return true
    else if ((_fib == that._fib) and (_count > that._count)) then
      return true
    else
      return false
    end

  fun box gte (that: box->FibonacciHitCounter): Bool =>
    eq(that) or gt(that)

  fun box lt (that: box->FibonacciHitCounter): Bool =>
    if (_fib > that._fib) then
      return false
    else if (_fib < that._fib) then
      return true
    else if ((_fib == that._fib) and (_count < that._count)) then
      return true
    else
      return false
    end

  fun box lte (that: box->FibonacciHitCounter): Bool =>
    eq(that) or lt(that)
