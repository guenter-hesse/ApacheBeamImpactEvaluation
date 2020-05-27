package de.hpi.epic.streambenchmark.helper;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * The Class Statistic.
 */
public class StatisticWrapper {

	/** The min. */
	double min;

	/** The max. */
	double max;

	/** The average. */
	double average;

	/** The sum. */
	double sum;

	/** The count. */
	int count;

	/**
	 * Instantiates a new statistic.
	 */
	public StatisticWrapper() {
		this.min = Double.MAX_VALUE;
		this.max = Double.MIN_VALUE;
		this.average = 0.0;
		this.sum = 0.0;
		this.count = 0;
	}

	/**
	 * Adds the value.
	 *
	 * @param value
	 *            the value
	 * @return the tuple4
	 */
	public Tuple4<Double, Double, Double, Double> addValue(final double value) {
		if (value < this.min) {
			this.min = value;
		}
		if (value > this.max) {
			this.max = value;
		}
		this.sum += value;
		this.count++;
		this.average = this.average * (this.count - 1) / this.count + value / this.count;

		return new Tuple4<Double, Double, Double, Double>(this.min, this.max, this.average, this.sum);
	}

}
