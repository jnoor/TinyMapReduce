package edu.ucla.cs.jet.mapreduce;

import java.util.Iterator;

/**
 * Created by jnoor on 12/13/16.
 */

public interface Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public void reduce(KEYIN key, Iterator<VALUEIN> value, OutputCollector<KEYOUT, VALUEOUT> out);
}
