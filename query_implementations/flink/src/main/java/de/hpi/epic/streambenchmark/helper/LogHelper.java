package de.hpi.epic.streambenchmark.helper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;

/**
 * The Class LogHelper.
 */
public class LogHelper {

	/** The Constant DATE_FORMAT. */
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	/** The Constant FLUSH_LINE_THRESHOLD. */
	private static final int FLUSH_LINE_THRESHOLD = 1000;

	/** The benchmark name. */
	private final String benchmarkName;

	/** The log path. */
	private final FileOutputStream logStream;

	/** The log line counter. */
	private int logLineCounter;

	/**
	 * Instantiates a new log helper.
	 *
	 * @param name
	 *            the name
	 * @param logPath
	 *            the log path
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public LogHelper(final String name, final String logPath) throws FileNotFoundException {
		this.benchmarkName = name;
		this.logStream = new FileOutputStream(logPath + this.benchmarkName + System.currentTimeMillis() + ".log", true);
		this.logLineCounter = 0;
	}

	/**
	 * Logs the message in the given log file.
	 *
	 * @param message
	 *            the message
	 */
	public void log(final String message) {
		try {
			this.logStream.write((DATE_FORMAT.format(new Date()) + "\t-\t" + message + "\n").getBytes());
			if (++this.logLineCounter > FLUSH_LINE_THRESHOLD) {
				this.logStream.flush();
				this.logLineCounter = 0;
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Log result.
	 *
	 * @param result
	 *            the result
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void logResult(final JobExecutionResult result) throws IOException {
		this.log("Finished Benchmark: " + this.benchmarkName + ". Job took " + formatResultTime(result)
				+ " hours to execute.");
		this.logStream.flush();
		this.logStream.close();
	}

	/**
	 * Format result time.
	 *
	 * @param result
	 *            the result
	 * @return the string
	 */
	private static String formatResultTime(final JobExecutionResult result) {
		long millis = result.getNetRuntime(TimeUnit.MILLISECONDS);
		final long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		final long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes);
		final long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
		millis -= TimeUnit.SECONDS.toMillis(seconds);
		return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis);
	}
}
