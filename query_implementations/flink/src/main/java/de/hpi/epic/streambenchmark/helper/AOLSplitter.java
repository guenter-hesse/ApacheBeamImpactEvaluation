package de.hpi.epic.streambenchmark.helper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

/**
 * The Class AOLSplitter.
 */
public class AOLSplitter implements FlatMapFunction<String, Tuple5<String, String, String, String, String>> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
	 * org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap(final String value, final Collector<Tuple5<String, String, String, String, String>> out)
			throws Exception {
		final String[] cols = value.split("\\t", -1);
		if (cols.length == 5) {
			out.collect(new Tuple5<>(cols[0], cols[1], cols[2], cols[3], cols[4]));
		}
	}

}
