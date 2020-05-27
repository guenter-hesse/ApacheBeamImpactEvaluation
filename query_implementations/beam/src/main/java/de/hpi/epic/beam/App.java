package de.hpi.epic.beam;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import de.hpi.epic.beam.benchmarks.Benchmark;
import de.hpi.epic.beam.benchmarks.Grep;
import de.hpi.epic.beam.benchmarks.Identity;
import de.hpi.epic.beam.benchmarks.Projection;
import de.hpi.epic.beam.benchmarks.Sample;
import de.hpi.epic.beam.options.Options;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;

public class App {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(final String[] args) {

		final Options options = createOptions(args);
		final Benchmark benchmark;

		switch (options.getBenchmark()) {
			case "Grep":
				benchmark = new Grep(options);
				break;
			case "Identity":
				benchmark = new Identity(options);
				break;
			case "Projection":
				benchmark = new Projection(options);
				break;
			case "Sample":
				benchmark = new Sample(options);
				break;
			default:
				benchmark = new Sample(options);
				options.setBenchmark("Sample");
				break;
		}

		final String outputTopic = "TOPIC_NAME_WITH_INDEX_SUFFIX_" + options.getRun();
		final String inputTopic = "INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_" + options.getRun();
		final Pipeline p = Pipeline.create(options);
        	KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
				.withBootstrapServers("xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx")
				.withTopics(Collections.singletonList(inputTopic))
				.withKeyDeserializer(StringDeserializer.class)
				.withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.<String, Object>of("auto.offset.reset", "earliest")) 
                                .updateConsumerProperties(ImmutableMap.<String, Object>of("group.id", "beamapexi_" + options.getBenchmark().toLowerCase() + options.getParallelism() + options.getRun()))
		;

		KafkaIO.Write<Void, String> writeToKafka = KafkaIO.<Void, String>write()
				.withBootstrapServers("xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx")
				.withTopic(outputTopic)
				.withValueSerializer(StringSerializer.class); // just need serializer for value

		p.apply(read.withoutMetadata())
		.apply(Values.<String>create())
		.apply(options.getBenchmark(), benchmark)
		.apply(writeToKafka.values());
		p.run().waitUntilFinish();
	}

	/**
	 * Creates the options from arguments.
	 *
	 * @param args
	 *            the args
	 * @return the option
	 */
	public static Options createOptions(final String[] args) {
        	return PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
	}

}
