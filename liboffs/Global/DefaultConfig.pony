use "Config"
primitive DefaultConfig
  fun apply(): Config val =>
    recover val
      let config = Config
      config("indexNodeSize") = USize(25)
      config("sectionSize") = USize(25)
      config("cacheCount") = USize(50)
      config("maxTupleSize") = USize(5)
      config("minTupleSize") = USize(2)
      config("tupleSize") = USize(3)
      config("lruSize") = USize(20)
      config("descriptorPad") = USize(32)
      config
    end
