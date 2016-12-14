# TinyMapReduce: Mobile MapReduce

TinyMapReduce is an MapReduce engine for processing data stored in Pebbles.

## Usage:

```
//Implement a Mapper
public class PebblesAveragePerTopicMapper implements Mapper<Integer, Integer> {
    @Override
    public void map(int topic, byte [] value, OutputCollector<Integer, Integer> out) {
        int val = ByteBuffer.wrap(value).getInt();
        out.collect(topic, val);
    }
}

//Implement a Reducer
public class PebblesAveragePerTopicReducer implements Reducer<Integer, Integer, Integer, Double> {
    @Override
    public void reduce(Integer key, Iterator<Integer> value, OutputCollector<Integer, Double> out) {
        double sum = 0.0;
        int cnt = 0;
        while(value.hasNext()) {
            cnt++;
            sum += value.next();
        }
        out.collect(key, sum/cnt);
    }
}

TinyMapReduce tiny = new TinyMapReduce(new PebblesAveragePerTopicMapper(), new PebblesAveragePerTopicReducer());
tiny.setOutputFile(new File("/path/to/desired/output/file.txt"));
tiny.runBatch(0, Pebbles.getInstance().offset());


```