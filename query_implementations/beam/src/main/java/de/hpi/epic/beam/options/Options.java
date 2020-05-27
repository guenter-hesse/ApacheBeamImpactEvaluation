package de.hpi.epic.beam.options;

import org.apache.beam.runners.flink.DefaultParallelismFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.apex.ApexPipelineOptions;
/**
 * The Interface Options.
 */
public interface Options extends PipelineOptions {

	@Description("configuration properties file for the Apex engine")
	void setConfigFile(String name);

	@org.apache.beam.sdk.options.Default.String("classpath:/beam-runners-apex.properties")
	String getConfigFile();

	Boolean getEmbeddedExecution();

	void setEmbeddedExecution(Boolean embedded);

	String getApplicationName();

	void setApplicationName(String name);

	@Default.InstanceFactory(value=SparkPipelineOptions.TmpCheckpointDirFactory.class)
	String getCheckpointDir();

	void setCheckpointDir(String checkpointDir);

	@Description("The degree of parallelism to be used when distributing operations onto workers.")
	@Default.InstanceFactory(DefaultParallelismFactory.class)
	Integer getParallelism();

	void setParallelism(Integer value);

        @Description("Number of run for query")
	@Default.String("1")
	String getRun();

	void setRun(String value);

	@Description("Benchmark to be executed")
	@Default.String("DistinctCount")
	String getBenchmark();

	void setBenchmark(String value);

	@Description("Path of the file to read from")
	@Default.String("")
	String getInputFile();

	void setInputFile(String value);

	@Description("The host being listened")
	@Default.String("localhost")
	String getHost();

	void setHost(String value);

	@Description("The port being listened")
	@Default.Integer(9999)
	int getPort();

	void setPort(int value);

	@Description("Path of the file to write to")
	@Default.String("./")
	String getOutput();

	void setOutput(String value);
	
        @Description("Number of the column to project to")
	@Default.Integer(1)
	int getProjectionColumn();

	void setProjectionColumn(int value);

	@Description("Word the input should contain for the grep benchmark")
	@Default.String("test")
	String getGrepValue();

	void setGrepValue(String value);

	@Description("The default probability for the sample benchmark in percent.")
	@Default.Double(40.0)
	double getSampleProbability();

	void setSampleProbability(double value);
}
