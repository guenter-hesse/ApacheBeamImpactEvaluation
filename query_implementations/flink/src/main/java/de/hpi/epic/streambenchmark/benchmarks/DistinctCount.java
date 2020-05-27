package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import java.util.HashSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import de.hpi.epic.streambenchmark.helper.AOLSplitter;

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
public class DistinctCount extends Benchmark {

	/**
	 * Instantiates a new distinct count.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public DistinctCount(final String[] args) throws FileNotFoundException {
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
		final DataStream<Tuple5<String, String, String, String, String>> splittedStream = inputStream
				.flatMap(new AOLSplitter());
		final DataStream<Tuple1<String>> projectedStream = splittedStream.project(1);
		final DataStream<Integer> distinctCount = projectedStream.flatMap(new WordSplitter());
		return distinctCount;
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class WordSplitter implements FlatMapFunction<Tuple1<String>, Integer> {

		/** The Constant words. */
		private static final HashSet<String> words = new HashSet<String>();

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
		 * org.apache.flink.util.Collector)
		 */
		@Override
		public void flatMap(final Tuple1<String> value, final Collector<Integer> out) {
			words.add(value.f0);
			out.collect(words.size());
		}
	}
}
