package sb_metric_calc

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.Date
import java.text.SimpleDateFormat

object ResultWriter {

  private val sdf = new SimpleDateFormat("yyyy_mm_dd__HH_mm_ss")
  private val resultsDir = "results"
  private val header = "query;topic;firstTS;lastTS;diff;sum;avg\n"
  if (!Files.exists(Paths.get(resultsDir)))
    Files.createDirectories(Paths.get(resultsDir));
  val resultFile: Path = Paths.get(s"$resultsDir/${sdf.format(new Date())}.csv")
  Files.write(resultFile, header.getBytes, StandardOpenOption.CREATE)

  def write(res: Map[String, Double]): Unit = {
    println(res.mkString("", ";", "\n"))
    Files.write(resultFile, res.mkString("", ";", "\n").getBytes, StandardOpenOption.APPEND)
  }

  def write(res: List[String]): Unit = {
    println(res.mkString("", ";", "\n"))
    Files.write(resultFile, res.mkString("", ";", "\n").getBytes, StandardOpenOption.APPEND)
  }

}
