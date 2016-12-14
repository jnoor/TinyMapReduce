package edu.ucla.cs.jet.mapreduce;

import android.util.Log;
import android.util.Pair;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.ucla.cs.jet.pebbleslib.DataEntry;
import edu.ucla.cs.jet.pebbleslib.Pebbles;

/**
 * Created by jnoor on 12/13/16.
 */

public class TinyMapReduce {

    private Mapper mapper;
    private Reducer reducer;

    private HashMap map_map;
    private HashMap reduce_map;

    private File resultFile;

    public class Collector<K,V> implements OutputCollector<K, V> {
        ArrayDeque<Pair<K, V>> q;
        public Collector() {
            q = new ArrayDeque<>();
        }
        public void collect(K key, V val) {
            q.add(new Pair<>(key, val));
        }
    }

    public TinyMapReduce(Mapper mp, Reducer rd) {
        mapper = mp;
        reducer = rd;

        resultFile = null;

        map_map = new HashMap<>();
        reduce_map = new HashMap<>();
    }

    public void setOutputFile(File f) {
        this.resultFile = f;
    }

    public void runBatch(long start, long end) {
        Pebbles pb = Pebbles.getInstance();
        Iterator<DataEntry> it = pb.read(start, end);

        //perform MAP
        Collector collector = new Collector<>();
        while(it.hasNext()) {

            DataEntry entry = it.next();

            mapper.map(entry.topic, entry.value, collector);

            //process map results
            Iterator<Pair> results = collector.q.iterator();
            while(results.hasNext()) {
                Pair p = results.next();
                if (map_map.containsKey(p.first)) {
                    ((ArrayDeque) map_map.get(p.first)).add(p.second);
                } else {
                    ArrayDeque dq = new ArrayDeque<>();
                    dq.add(p.second);
                    map_map.put(p.first, dq);
                }
            }

            //clear for next cycle
            collector.q.clear();
        }

        //perform REDUCE
        Collector collect = new Collector<>();
        Iterator<HashMap.Entry> it2 = map_map.entrySet().iterator();
        while (it2.hasNext()) {

            HashMap.Entry pair = it2.next();

            reducer.reduce(pair.getKey(), ((ArrayDeque) pair.getValue()).iterator(), collect);

            //process reduce results
            Iterator<Pair> it3 = collect.q.iterator();
            while(it3.hasNext()) {
                Pair p = it3.next();
                reduce_map.put(p.first, p.second);
            }

            //clear for next cycle
            collect.q.clear();
        }

        //print results
        Iterator final_it = reduce_map.entrySet().iterator();
        PrintWriter writer = null;
        if (resultFile != null) {
            try {
                writer = new PrintWriter(resultFile);
            } catch (Exception e) {}
        }
        while(final_it.hasNext()) {
            Map.Entry e = (Map.Entry) final_it.next();
            Log.i("RESULTS", String.valueOf(e.getKey()) + ":" + String.valueOf(e.getValue()));
            if(writer != null) {
                try {
                    writer.println(e.getKey() + "," + String.valueOf(e.getValue()));
                } catch (Exception ex) {}
            }
        }
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {}
        }
    }
}
