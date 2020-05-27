package de.hpi.epic.streambenchmark.helper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

/**
 * The Class AOLSplitter.
 */
public class NumberSplitter implements FlatMapFunction<String, Tuple5<Double, Double, Double, Double, Double>> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
	 * org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap(final String value, final Collector<Tuple5<Double, Double, Double, Double, Double>> out)
			throws Exception {
		final String[] cols = value.split("\\t");
		if (cols.length == 5) {
			final double value0 = Double.parseDouble(cols[0].replace(",", "."));
			final double value1 = Double.parseDouble(cols[1].replace(",", "."));
			final double value2 = Double.parseDouble(cols[2].replace(",", "."));
			final double value3 = Double.parseDouble(cols[3].replace(",", "."));
			final double value4 = Double.parseDouble(cols[4].replace(",", "."));
			out.collect(new Tuple5<>(value0, value1, value2, value3, value4));
		}
	}

}
