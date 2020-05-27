package util

import scala.collection.mutable.Queue

import java.io.File
import java.nio.charset.Charset
import com.google.common.io.Files
import util.ResultWriter

// Logging Services
// Writes entries to log file with unix timestamp

class LogWriter extends ResultWriter {

  override def addEntry(entry: String) {
    synchronized {
      backLog += "[" + System.currentTimeMillis + "]" + entry
    }
  }
}
