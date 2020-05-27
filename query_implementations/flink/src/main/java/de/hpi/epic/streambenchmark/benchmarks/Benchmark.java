package de.hpi.epic.streambenchmark.benchmarks;

import java.io.FileNotFoundException;
import java.util.Properties;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import de.hpi.epic.streambenchmark.helper.LogHelper;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * The Class Benchmark.
 */
public abstract class Benchmark {

	/** The benchmark name. */
	protected final String benchmarkName;

	/** The hostname. */
	protected final String hostname;

	/** The port. */
	protected final int port;
	protected final int numberOfRun;

	/** The log path. */
	protected final LogHelper logger;

	/** The result path. */
	protected final String resultPath;
	Properties properties = new Properties();
	final String BOOTSTRAP_SERVERS = "xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx";

	/**
	 * Instantiates a new benchmark.
	 *
	 * @param args
	 *            the args
	 * @throws FileNotFoundException
	 *             the file not found exception
	 */
	protected Benchmark(final String[] args) throws FileNotFoundException {
		this.benchmarkName = args[0];
		this.hostname = args[1];
		this.port = Integer.parseInt(args[2]);
		this.logger = new LogHelper(this.benchmarkName, args[3]);
		this.resultPath = args[3] + this.benchmarkName + System.currentTimeMillis() + ".res";
		this.numberOfRun = Integer.parseInt(args[4]);
		this.properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
		this.properties.setProperty("group.id", "sb_flink");
	}

	/**
	 * Gets the environment.
	 *
	 * @return the environment
	 */
	protected StreamExecutionEnvironment getEnvironment() {
		return StreamExecutionEnvironment.getExecutionEnvironment();
	}

	/**
	 * Gets the input stream.
	 *
	 * @param env
	 *            the env
	 * @return the input stream
	 */
	protected DataStream<String> getInputStream(final StreamExecutionEnvironment env) {
		FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_" + this.numberOfRun, new SimpleStringSchema(), this.properties);
		myConsumer.setStartFromEarliest();
		return env.addSource(myConsumer);
		//return env.socketTextStream(this.hostname, this.port);
	}

	protected SinkFunction getSink() {
		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
				BOOTSTRAP_SERVERS,            // broker list
				"TOPIC_NAME_WITH_INDEX_SUFFIX_" + this.numberOfRun,                  // target topic
				new SimpleStringSchema());   // serialization schema

		// versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
		// this method is not available for earlier Kafka versions
		//myProducer.setWriteTimestampToKafka(true);
		return myProducer;
	}

	/**
	 * Executes the benchmark.
	 *
	 * @throws Exception
	 *             the exception
	 */
	public void execute() throws Exception {
		this.logger.log("Setting up benchmark: " + this.benchmarkName);
		this.logger.log("Setting up stream environment.");
		final StreamExecutionEnvironment env = this.getEnvironment();
		final DataStream<String> inputStream = this.getInputStream(env);

		this.setupStream(inputStream).addSink(getSink());//.writeAsText(this.resultPath, FileSystem.WriteMode.OVERWRITE);

		this.logger.log("Starting benchmark: " + this.benchmarkName);
		this.logger.logResult(env.execute(this.benchmarkName));
	}

	/**
	 * The setup stream.
	 *
	 * @param inputStream
	 *            the input stream
	 * @return the data stream
	 */
	protected abstract DataStream<?> setupStream(DataStream<String> inputStream);

}
