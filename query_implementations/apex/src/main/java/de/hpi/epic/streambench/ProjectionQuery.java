/**
 * Put your copyright and license info here.
 */
package de.hpi.epic.streambench;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class ProjectionQuery extends BaseOperator implements Query<String> {

    public ProjectionQuery() {
    }

    @Override
    public DefaultInputPort<String> getInput() {
        return input;
    }

    @Override
    public DefaultOutputPort<String> getOutput() {
        return output;
    }

    /**
     * Defines whether to send data to the output port after each tuple or each window
     */
    private final int columnIdx = 0;

    /**
     * Defines Input Port - DefaultInputPort
     * Accepts data from the upstream operator
     * Type String
     */
    public transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
        /*
         * Its is a good idea to take the processing logic out of the process() call.
         * This allows for extending this operator into a different behavior by overriding processTuple() call.
         */
        @Override
        public void process(String tuple) {
            processTuple(tuple);
        }
    };
    public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    @Override
    public void beginWindow(long windowId) {
    }

    public void processTuple(String tuple) {
	output.emit(tuple.split("\\t", -1)[columnIdx]);
    }
}
