package edu.ucla.cs.jet.mapreduce;

/**
 * Created by jnoor on 12/13/16.
 */

public interface OutputCollector<K,V> {
    public void collect(K key, V value);
}
