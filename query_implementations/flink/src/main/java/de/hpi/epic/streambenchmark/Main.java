package de.hpi.epic.streambenchmark;

import de.hpi.epic.streambenchmark.benchmarks.DistinctCount;
import de.hpi.epic.streambenchmark.benchmarks.Grep;
import de.hpi.epic.streambenchmark.benchmarks.GrepRegex;
import de.hpi.epic.streambenchmark.benchmarks.Identity;
import de.hpi.epic.streambenchmark.benchmarks.Projection;
import de.hpi.epic.streambenchmark.benchmarks.ProjectionNonStatic;
import de.hpi.epic.streambenchmark.benchmarks.Sample;
import de.hpi.epic.streambenchmark.benchmarks.Statistics;
import de.hpi.epic.streambenchmark.benchmarks.WordCount;

/**
 * The Class Main.
 */
public class Main {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(final String[] args) throws Exception {
		if (args.length == 5) {
			switch (args[0]) {
				case "DistinctCount":
					new DistinctCount(args).execute();
					break;
				case "Grep":
					new Grep(args).execute();
					break;
				case "GrepRegex":
					new GrepRegex(args).execute();
					break;
				case "Identity":
					new Identity(args).execute();
					break;
				case "Projection":
					new Projection(args).execute();
					break;
				case "ProjectionNonStatic":
					new ProjectionNonStatic(args).execute();
					break;
				case "Sample":
					new Sample(args).execute();
					break;
				case "Statistics":
					new Statistics(args).execute();
					break;
				case "WordCount":
					new WordCount(args).execute();
				default:
					System.err.println(getErrorMessage());
					break;
			}
		} else {
			System.err.println(getErrorMessage());
		}
	}

	private static String getErrorMessage() {
		return "Usage: app <benchmark> <hostname> <port> <outputPath>\nWhere <benchmark> is one of:\n\tDistinctCount"
				+ "\n\tGrep\n\tGrepRegex\n\tIdentity\n\tProjection\n\tProjectionNonStatic\n\tSample\n\tStatistics"
				+ "\n\tWordCount";
	}
}
