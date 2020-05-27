package de.hpi.epic.beam.benchmarks;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import de.hpi.epic.beam.options.Options;

/**
 * The Class Projection.
 */
public class Projection extends Benchmark {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/**
	 * Instantiates a new projection.
	 *
	 * @param options
	 *            the options
	 */
	public Projection(final Options options) {
		super(options);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.google.cloud.dataflow.sdk.transforms.PTransform#apply(com.google.cloud.dataflow.sdk.values.PInput)
	 */
	@Override
	public PCollection<String> expand(final PCollection<String> lines) {
		return lines.apply(ParDo.of(new ProjectLines(this.options.getProjectionColumn())));
	}

	/**
	 * The Class ExtractWordsFn.
	 */
	public static class ProjectLines extends DoFn<String, String> {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/** The projection column. */
		private final int projectionColumn;

		/**
		 * Instantiates a new project.
		 *
		 * @param projectionColumn
		 *            the projection column
		 */
		public ProjectLines(final int projectionColumn) {
			this.projectionColumn = projectionColumn;
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
			c.output(c.element().split("\\t", -1)[this.projectionColumn]);
		}
	}
}
