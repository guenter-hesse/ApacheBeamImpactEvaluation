package de.hpi.epic.beam.benchmarks;

import org.apache.beam.sdk.values.PCollection;

import de.hpi.epic.beam.options.Options;

/**
 * The Class Identity.
 */
public class Identity extends Benchmark {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	public Identity(final Options options) {
		super(options);
	}

	@Override
	public PCollection<String> expand(final PCollection<String> lines) {
		return lines;
	}
}
