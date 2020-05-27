package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class Projection extends Benchmark {

	/**
	 * Instantiates a new projection.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public Projection(final String[] args) throws FileNotFoundException {
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
		final DataStream<String> projectedStreamAsString = projectedStream.map(new MapFunction<Tuple1<String>, String>() {
			@Override
			public String map(Tuple1<String> stringTuple1) throws Exception {
				return stringTuple1.toString();
			}
		});
		return projectedStreamAsString;
	}
}
