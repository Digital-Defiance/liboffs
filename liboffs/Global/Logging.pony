use "logger"
use "../OFFStreams"
use "Base58"
use "files"

primitive CreateLogger
  fun apply(filename: String, env: Env): Logger[String val] val  =>
    let path: FilePath = FilePath(env.root, filename)
    let logFile: File iso = recover File(path) end
    let logFileStream: FileStream = FileStream(consume logFile)
    StringLogger(Info, logFileStream)


primitive LogTuple
  fun apply(tuple: Tuple val, logger: Logger[String]) =>
    logger.log(
      recover
        let txt = String(100)
        txt.append("(")
        var comma = false
        for hash in tuple.values() do
          if comma then
            txt.append(",")
          end
          comma = true
          txt.append(try recover Base58.encode(hash.data)? end else "" end)
        end
        txt.append(")")
        txt
      end
        )
