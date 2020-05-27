package de.hpi.epic.beam.benchmarks;

import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import de.hpi.epic.beam.options.Options;

/**
 * The Class Sample.
 */
public class Sample extends Benchmark {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * Instantiates a new sample.
	 *
	 * @param options
	 *            the options
	 */
	public Sample(final Options options) {
		super(options);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.cloud.dataflow.sdk.transforms.PTransform#apply(com.google.cloud.dataflow.sdk.values.PInput)
	 */
	@Override
	public PCollection<String> expand(final PCollection<String> lines) {
		return lines.apply(ParDo.of(new SampleLines(this.options.getSampleProbability())));
	}

	/**
	 * The Class ExtractWordsFn.
	 */
	public static class SampleLines extends DoFn<String, String> {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;
		/** The grep value. */
		private final double sampleProbability;

		/** The random. */
		private final Random random;

		/**
		 * Instantiates a new grep lines.
		 *
		 * @param sampleProbability
		 *            the sample probability
		 */
		public SampleLines(final double sampleProbability) {
			this.sampleProbability = sampleProbability % 100; // = 40.0 / 100
			this.random = new Random();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * com.google.cloud.dataflow.sdk.transforms.DoFn#processElement(com.google.cloud.dataflow.sdk.transforms.DoFn.
		 * ProcessContext)
		 */
		@ProcessElement
		public void processElement(final ProcessContext c) {
			if (this.random.nextInt(100) <= this.sampleProbability) {
				c.output(c.element());

			}
		}
	}

}
