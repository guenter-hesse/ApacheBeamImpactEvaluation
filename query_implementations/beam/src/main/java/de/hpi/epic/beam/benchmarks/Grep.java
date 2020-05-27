package de.hpi.epic.beam.benchmarks;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import de.hpi.epic.beam.options.Options;

/**
 * The Class Grep.
 */
public class Grep extends Benchmark {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	public Grep(final Options options) {
		super(options);
	}

	@Override
	public PCollection<String> expand(final PCollection<String> lines) {
		return lines.apply(ParDo.of(new GrepLines(this.options.getGrepValue())));
	}

	public static class GrepLines extends DoFn<String, String> {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;
		/** The grep value. */
		private final String grepValue;

		/**
		 * Instantiates a new grep lines.
		 *
		 * @param grepValue
		 *            the grep value
		 */
		public GrepLines(final String grepValue) {
			this.grepValue = grepValue;
		}

		/**
		 * Process element.
		 *
		 * @param c
		 *            the c
		 */
		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.google.cloud.dataflow.sdk.transforms.DoFn#processElement(com.google.cloud.dataflow.sdk.transforms.DoFn.
		 * ProcessContext)
		 */
		@ProcessElement
		public void processElement(final ProcessContext c) {
			if (c.element().contains(this.grepValue)) {
				c.output(c.element());
			}
		}
	}
}
