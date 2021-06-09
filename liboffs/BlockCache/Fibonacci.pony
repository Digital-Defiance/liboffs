use "json"
primitive Fibonacci
  fun apply(num: U64) : U64 =>
    if ((num == 1) or (num == 0)) then
      num
    else
      apply(num - 1) + apply(num - 2)
    end

class FibonacciHitCounter
  var _fib : U64 = 0
  var _count: U64 = 0
  var _threshold: U64

  new create() =>
    _threshold = Fibonacci(_fib)

  new from(fib': U64, count': U64) =>
    _fib = fib'
    _count = count'
    _threshold = Fibonacci(_fib)

  new fromJSON(obj: JsonObject val)? =>
    _fib = (obj.data("fib")? as I64).u64()
    _count = (obj.data("count")? as I64).u64()
    _threshold = (obj.data("threshold")? as I64).u64()

  fun toJSON(): JsonObject =>
    let obj = JsonObject
    obj.data("fib") = _fib.i64()
    obj.data("count") = _count.i64()
    obj.data("threshold") = _threshold.i64()
    obj

  fun fib() : U64 val =>
    _fib

  fun count() : U64 val =>
    _count

  fun threshold() : U64 val =>
    _threshold

  fun ref increment(): Bool =>
    _count = _count + 1
    if (_count >= _threshold) then
      _fib = _fib + 1
      _count = 0
      _threshold = Fibonacci(_fib)
      return true
    end
    false

  fun ref decrement(): Bool  =>
    if _fib == 0 then
      if _count == 0 then
        return false
      else
        _count = _count - 1
        return false
      end
    end

    let threshold': U64 = Fibonacci(_fib - 1)
    if (_count - 1) < threshold' then
      _threshold = threshold'
      _count = threshold' - 1
      _fib = _fib - 1
      return true
    else
      _count = _count - 1
      return false
    end

  fun box eq(that: box->FibonacciHitCounter): Bool =>
    if ((that._fib == _fib) and (that._count == _count)) then
      true
    else
      false
    end

  fun box ne(that: box->FibonacciHitCounter): Bool =>
    not eq(that)

  fun box gt (that: box->FibonacciHitCounter): Bool =>
    if (_fib < that._fib) then
      return false
    elseif (_fib > that._fib) then
      return true
    elseif ((_fib == that._fib) and (_count > that._count)) then
      return true
    else
      return false
    end

  fun box ge (that: box->FibonacciHitCounter): Bool =>
    eq(that) or gt(that)

  fun box lt (that: box->FibonacciHitCounter): Bool =>
    if (_fib > that._fib) then
      return false
    elseif (_fib < that._fib) then
      return true
    elseif ((_fib == that._fib) and (_count < that._count)) then
      return true
    else
      return false
    end

  fun box le (that: box->FibonacciHitCounter): Bool =>
    eq(that) or lt(that)
