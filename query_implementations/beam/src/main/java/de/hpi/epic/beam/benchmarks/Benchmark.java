package de.hpi.epic.beam.benchmarks;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import de.hpi.epic.beam.options.Options;

/**
 * The Class Benchmark.
 */
public abstract class Benchmark extends PTransform<PCollection<String>, PCollection<String>> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The options. */
	protected final Options options;

	/**
	 * Instantiates a new benchmark.
	 *
	 * @param options
	 *            the options
	 */
	public Benchmark(final Options options) {
		this.options = options;
	}
}
