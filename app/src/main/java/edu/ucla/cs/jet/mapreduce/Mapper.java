package edu.ucla.cs.jet.mapreduce;

/**
 * Created by jnoor on 12/13/16.
 */

public interface Mapper<KEYOUT, VALUEOUT> {
    public void map(int topic, byte[] value, OutputCollector<KEYOUT, VALUEOUT> out);
}
