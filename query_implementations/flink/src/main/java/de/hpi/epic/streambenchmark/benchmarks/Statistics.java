package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import de.hpi.epic.streambenchmark.helper.NumberSplitter;
import de.hpi.epic.streambenchmark.helper.StatisticWrapper;

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
public class Statistics extends Benchmark {

	/**
	 * Instantiates a new statistics.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public Statistics(final String[] args) throws FileNotFoundException {
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
		final DataStream<Tuple5<Double, Double, Double, Double, Double>> splittedStream = inputStream
				.flatMap(new NumberSplitter());
		final DataStream<Tuple1<Double>> projectedStream = splittedStream.project(0);
		final DataStream<Tuple4<Double, Double, Double, Double>> statisticStream = projectedStream
				.flatMap(new StatisticsMap());
		return statisticStream;
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class StatisticsMap
			implements FlatMapFunction<Tuple1<Double>, Tuple4<Double, Double, Double, Double>> {

		/** The Constant statistic. */
		private static final StatisticWrapper statistic = new StatisticWrapper();

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
		 * org.apache.flink.util.Collector)
		 */
		@Override
		public void flatMap(final Tuple1<Double> value, final Collector<Tuple4<Double, Double, Double, Double>> out) {
			out.collect(statistic.addValue(value.f0));
		}
	}
}
