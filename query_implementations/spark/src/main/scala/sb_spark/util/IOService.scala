package util

object IOService {

  private val logService = new LogWriter()
  private val resultService = new ResultWriter()

  private var running = false

  def log(entry: String): Unit = {
    if (running) {
      logService.addEntry(entry)
    }
  }

  def line(): Unit = {
    log("---------------------------")
  }

  def result(entry: String): Unit = {
    if (running) {
      resultService.addEntry(entry)
    }
  }

  def start(logFilePath: String, resultFilePath: String): Unit = {
    logService.start(logFilePath)
    resultService.start(resultFilePath)
    running = true
  }

  def stop(): Unit = {
    if (running) {
      logService.stop
      resultService.stop
      running = false
    }
  }

}
