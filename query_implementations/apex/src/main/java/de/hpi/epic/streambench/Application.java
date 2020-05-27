/**
 * Put your copyright and license info here.
 */
package de.hpi.epic.streambench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortExactlyOnceOutputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.BasicConfigurator;
import java.util.Properties;
import java.util.UUID;

import static com.datatorrent.api.Context.OperatorContext.MEMORY_MB;
import static com.datatorrent.api.Context.OperatorContext.VCORES;

@ApplicationAnnotation(name="ApexSPBench")
public class Application implements StreamingApplication
{
  private final String BOOTSTRAP_SERVERS = "xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx,xxx.xxx.xx.xxx:xxxx";
  private final String GREP_BENCHMARK = "grep";
  private final String IDENTITY_BENCHMARK = "identity";
  private final String PROJECTION_BENCHMARK = "projection";
  private final String SAMPLE_BENCHMARK = "sample";
  private String benchmark;
  private String parallelism;
  private String run;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    BasicConfigurator.configure();
    System.out.println("Starting Application");
    System.out.println("Config is: " +conf);
    benchmark = conf.get("benchmark");
    parallelism = conf.get("parallelism");
    run = conf.get("run");
    System.out.println("benchmark: \n" + benchmark + "\n\nparallelism:\n" + parallelism + "\n\nrun:\n" + run + "\n");
    dag.setAttribute(VCORES, Integer.parseInt(parallelism));
    dag.setAttribute(MEMORY_MB, 49152);
    if (benchmark  != null && parallelism != null) {
      System.out.println("Starting benchmark: " + benchmark + "\nwith parallelism: " + parallelism);

      KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());
      kafkaInput.setTopics(getInputTopic());
      kafkaInput.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
      kafkaInput.setClusters(BOOTSTRAP_SERVERS);

      Query benchmarkInstance = null;
      boolean needsDeserialization = true;
      if (benchmark.toLowerCase().equals(IDENTITY_BENCHMARK)) {
        needsDeserialization = false;
        benchmarkInstance = dag.addOperator("identityQuery", new IdentityQuery());
      }

      if (!needsDeserialization) {
        KafkaSinglePortExactlyOnceOutputOperator<byte[]> kafkaExactlyOnceOutputOperator =
                dag.addOperator("kafkaExactlyOnceOutputOperator", KafkaSinglePortExactlyOnceOutputOperator.class);
        kafkaExactlyOnceOutputOperator.setProperties(getKafkaProperties(needsDeserialization));
        kafkaExactlyOnceOutputOperator.setTopic(getOutputTopic());
        dag.addStream("readFromKafkaForwardToBenchmark", kafkaInput.outputPort, benchmarkInstance.getInput()).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("outputResults", benchmarkInstance.getOutput(), kafkaExactlyOnceOutputOperator.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
      } else {
        StringDeserializer stringDeserializer = dag.addOperator("deserializer", new StringDeserializer());
        KafkaSinglePortExactlyOnceOutputOperator<String> kafkaExactlyOnceOutputOperator =
                dag.addOperator("kafkaExactlyOnceOutputOperator", KafkaSinglePortExactlyOnceOutputOperator.class);
        kafkaExactlyOnceOutputOperator.setProperties(getKafkaProperties(needsDeserialization));
        kafkaExactlyOnceOutputOperator.setTopic(getOutputTopic());
        switch (benchmark.toLowerCase()) {
          case GREP_BENCHMARK:
            benchmarkInstance = dag.addOperator("query", new GrepQuery());
            break;
          case PROJECTION_BENCHMARK:
            benchmarkInstance = dag.addOperator("query", new ProjectionQuery());
            break;
          case SAMPLE_BENCHMARK:
            benchmarkInstance = dag.addOperator("query", new SampleQuery());
            break;
          default:
            System.err.println("No valid benchmark configured. benchmark configured: " + benchmark + "\nValid values: grep|identity|projection|sample");
            break;
        }
        dag.addStream("readFromKafkaForwardToStringDes", kafkaInput.outputPort, stringDeserializer.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("stringDesToBenchmark", stringDeserializer.output, benchmarkInstance.getInput()).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("outputResults", benchmarkInstance.getOutput(), kafkaExactlyOnceOutputOperator.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
      }
      } else {
      System.err.println("benchmark and parallelism must be set");
    }
  }

  private Properties getKafkaProperties(boolean needsDeserialization) {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    if (needsDeserialization) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    } else {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    }
    return props;
  }

  private String getInputTopic() {
    String topic = "INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_" + run;
    return topic;
  }

  private String getOutputTopic() {
    String topic = "TOPIC_NAME_WITH_INDEX_SUFFIX_" + run;
    return topic;
  }
}


