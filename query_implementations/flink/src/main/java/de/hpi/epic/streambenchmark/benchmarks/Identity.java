package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;

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
public class Identity extends Benchmark {

	/**
	 * Instantiates a new identity.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	public Identity(final String[] args) throws FileNotFoundException {
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
		return inputStream;
	}
}
