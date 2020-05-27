/**
 * Put your copyright and license info here.
 */
package de.hpi.epic.streambench;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.beans.Transient;
import java.io.Serializable;

/**
 * This is a simple operator that emits random number.
 */
public class IdentityQuery extends BaseOperator implements Query<byte[]>{

    private static final long serialVersionUID = 201306031549L;

    public IdentityQuery() {
    }

    @Override
    public DefaultInputPort<byte[]> getInput() {
        return input;
    }

    @Override
    public DefaultOutputPort<byte[]> getOutput() {
        return output;
    }

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
        public void process(byte[] tuple) {
            processTuple(tuple);
        }
    };

    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();

    @Override
    public void beginWindow(long windowId) {
    }

    public void processTuple(byte[] tuple) {
        output.emit(tuple);
    }
}
