package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import java.util.Random;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class Sample extends Benchmark {
	/**
	 * Instantiates a new sample.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public Sample(final String[] args) throws FileNotFoundException {
		super(args);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.hpi.epic.streambenchmark.benchmarks.Benchmark#setupStream(org.apache.flink.streaming.api.datastream.
	 * DataStream)
	 */
	@Override
	protected DataStream<?> setupStream(final DataStream<String> inputStream) {
		return inputStream.filter(new SampleFilter());
	}

	/**
	 * The Class SampleFiler.
	 */
	public static class SampleFilter implements FilterFunction<String> {

		/** The r. */
		private static Random r = new Random();
        private static double sampleProbability = 40.0 % 100;
		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FilterFunction#filter(java.lang.Object)
		 */
		@Override
		public boolean filter(final String value) throws Exception {
			return (r.nextInt(100) <= sampleProbability);
		}
	}
}
