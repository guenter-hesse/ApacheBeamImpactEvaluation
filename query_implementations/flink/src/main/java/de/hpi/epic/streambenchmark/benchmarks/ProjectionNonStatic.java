package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import de.hpi.epic.streambenchmark.helper.TSVSplitter;

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
public class ProjectionNonStatic extends Benchmark {

	/**
	 * Instantiates a new projection non static.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public ProjectionNonStatic(final String[] args) throws FileNotFoundException {
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
		final DataStream<ArrayList<String>> projectedStream = inputStream.flatMap(new TSVSplitter());
		projectedStream.flatMap(new NonStaticProjectionMap(2));
		return projectedStream;
	}

	/**
	 * The Class NonStaticProjectionMap.
	 */
	public static class NonStaticProjectionMap implements FlatMapFunction<ArrayList<String>, ArrayList<String>> {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/** The indices. */
		private final HashSet<Integer> indices;

		/**
		 * Instantiates a new non static projection map.
		 *
		 * @param values
		 *            the values
		 */
		public NonStaticProjectionMap(final Integer... values) {
			this.indices = new HashSet<>();
			this.indices.addAll(Arrays.asList(values));
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
		 * org.apache.flink.util.Collector)
		 */
		@Override
		public void flatMap(final ArrayList<String> value, final Collector<ArrayList<String>> out) throws Exception {
			final ArrayList<String> output = new ArrayList<>();
			for (final Integer i : this.indices) {
				if (i >= 0 && i < value.size()) {
					output.add(value.get(i));
				}
			}
			out.collect(output);
		}
	}
}
