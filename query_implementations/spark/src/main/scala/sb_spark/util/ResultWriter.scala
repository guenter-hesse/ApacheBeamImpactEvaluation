package util

import scala.collection.mutable.Queue

import java.io.File
import java.nio.charset.Charset
import com.google.common.io.Files

// Writes result to file

class ResultWriter {

  @volatile protected var backLog = Queue[String]()

  class WriteBackThread extends Runnable {
    var running = false
    var outputFile : File = null


    def startup(filename: String): Unit = {
      outputFile = new File(filename)
      if (outputFile.exists()) outputFile.delete()
      running = true
    }

    def run(): Unit = {
      while(running) {
        synchronized {
          if(!backLog.isEmpty) {
            Files.append(backLog.dequeue() + "\n", outputFile, Charset.defaultCharset())
          }
        }
        Thread sleep(100)
      }

      if(outputFile != null) {
        synchronized {
          while(!backLog.isEmpty) {
            Files.append(backLog.dequeue() + "\n", outputFile, Charset.defaultCharset())
          }
        }
      }
    }

    def stop(): Unit = {
      running = false
    }
  }

  private val writeBackThread = new WriteBackThread()

  def addEntry(entry: String) {
    synchronized {
      backLog += entry
    }
  }

  def start(outputPath: String) = {
    synchronized {
      backLog = new Queue[String]()
    }
    writeBackThread.startup(outputPath)
    (new Thread(writeBackThread)).start()
  }

  def stop() = {
    writeBackThread.stop()
  }
}
