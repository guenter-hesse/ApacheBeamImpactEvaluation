package de.hpi.epic.streambenchmark.helper;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * The Class AOLSplitter.
 */
public class TSVSplitter implements FlatMapFunction<String, ArrayList<String>> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flink.api.common.functions.FlatMapFunction#flatMap(java.lang.Object,
	 * org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap(final String value, final Collector<ArrayList<String>> out) throws Exception {
		final String[] values = value.split("\\t", -1);
		final ArrayList<String> valuesList = new ArrayList<>(values.length);
		valuesList.addAll(Arrays.asList(values));
		out.collect(valuesList);
	}
}
