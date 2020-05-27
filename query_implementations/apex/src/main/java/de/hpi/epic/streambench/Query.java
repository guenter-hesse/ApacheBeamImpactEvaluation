package de.hpi.epic.streambench;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public interface Query<T> {

    public DefaultInputPort<T> getInput();
    public DefaultOutputPort<T> getOutput();

}
