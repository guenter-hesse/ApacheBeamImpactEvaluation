/**
 * Put your copyright and license info here.
 */
package de.hpi.epic.streambench;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import java.util.Map;

/**
 * This is a simple operator that emits random number.
 */
public class StringDeserializer extends BaseOperator
{

  /**
   * Defines whether to send data to the output port after each tuple or each window
   */
  private boolean sendPerTuple = true; // Default

  /**
   * Defines Input Port - DefaultInputPort
   * Accepts data from the upstream operator
   * Type String
   */
  public transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>() {
    /*
     * Its is a good idea to take the processing logic out of the process() call.
     * This allows for extending this operator into a different behavior by overriding processTuple() call.
     */
    @Override
    public void process(byte[] tuple)
    {
	processTuple(tuple);
    }
  };

  /**
   * Defines Output Port - DefaultOutputPort
   * Sends data to the down stream operator which can consume this data
   * Type Map<String, Long>
   */
  public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  /**
   * Setup call
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
  }

  /**
   * Begin window call for the operator.
   * If sending counts per window, clear the counts at the start of the window
   * @param windowId
   */
  public void beginWindow(long windowId)
  {
  }

  /**
   * Defines what should be done with each incoming tuple
   * Update gobalCounts and updatedCounts.
   * If sending output per tuple, clear the updatedCounts and then send out the updatedCounts after processing the tuple
   */
  protected void processTuple(byte[] tuple)
  {
    output.emit(new org.apache.kafka.common.serialization.StringDeserializer().deserialize(null, tuple));
  }

  /**
   * End window call for the operator
   * If sending per window, emit the updated counts here.
   */
  @Override
  public void endWindow()
  {
  }

  /*
   * Getters and setters for Operator properties
   */
  public boolean isSendPerTuple()
  {
    return sendPerTuple;
  }

  public void setSendPerTuple(boolean sendPerTuple)
  {
    this.sendPerTuple = sendPerTuple;
  }

}
