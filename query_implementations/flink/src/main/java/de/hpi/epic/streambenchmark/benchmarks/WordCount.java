package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

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
public class WordCount extends Benchmark {

	/**
	 * Instantiates a new word count.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public WordCount(final String[] args) throws FileNotFoundException {
		super(args);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.hpi.epic.streambenchmark.benchmarks.Benchmark#setupStream(org.apache.flink.streaming.api.datastream.
	 * DataStream)
	 */
	@Override
	protected DataStream<?> setupStream(DataStream<String> inputStream) {
		inputStream = inputStream.flatMap(new WordSplitter());
		final DataStream<Tuple2<String, Integer>> wordCount = inputStream.flatMap(new WordCounter());
		return wordCount;
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class WordSplitter implements FlatMapFunction<String, String> {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/**
		 * Flat map.
		 *
		 * @param value
		 *            the value
		 * @param out
		 *            the out
		 */
		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
		 * org.apache.flink.util.Collector)
		 */
		@Override
		public void flatMap(final String value, final Collector<String> out) {
			final String[] tokens = value.toLowerCase().split(" "/* "\\W+" */);
			for (final String token : tokens) {
				if (token.length() > 0) {
					out.collect(token);
				}
			}
		}
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class WordCounter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/** The Constant wordCount. */
		private static final HashMap<String, Integer> wordCount = new HashMap<>();

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
		 * org.apache.flink.util.Collector)
		 */
		@Override
		public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) {
			Integer count;
			if (wordCount.containsKey(value)) {
				count = wordCount.get(value).intValue() + 1;
				wordCount.put(value, count);
			} else {
				count = 1;
				wordCount.put(value, count);
			}
			out.collect(new Tuple2<>(value, count));
		}
	}
}
