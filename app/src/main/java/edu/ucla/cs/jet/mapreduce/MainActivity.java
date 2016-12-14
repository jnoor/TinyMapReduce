package edu.ucla.cs.jet.mapreduce;

import android.Manifest;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.os.Environment;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.ucla.cs.jet.pebbleslib.Pebbles;

public class MainActivity extends AppCompatActivity {

    boolean permission_granted = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        if (ContextCompat.checkSelfPermission(getApplicationContext(), Manifest.permission.WRITE_EXTERNAL_STORAGE) != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE, Manifest.permission.READ_EXTERNAL_STORAGE}, 1);
        } else {
            permission_granted = true;
        }
    }

    public class PebblesAveragePerTopicMapper implements Mapper<Integer, Integer> {
        @Override
        public void map(int topic, byte [] value, OutputCollector<Integer, Integer> out) {
            int val = ByteBuffer.wrap(value).getInt();
            out.collect(topic, val);
        }
    }

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

    @Override
    protected void onResume() {
        super.onResume();
        if (permission_granted) {
            writeData();
            TinyMapReduce tiny = new TinyMapReduce(new PebblesAveragePerTopicMapper(), new PebblesAveragePerTopicReducer());

            File sdCard = Environment.getExternalStorageDirectory();
            sdCard.mkdirs();
            File directory = new File(sdCard.getAbsolutePath(), "TinyMapReduce");
            directory.mkdirs();
            File results = new File(directory, "results.txt");
            tiny.setOutputFile(results);

            tiny.runBatch(0, Pebbles.getInstance().offset());
        }
    }

    private void writeData() {
        Pebbles pb = Pebbles.getInstance();
        pb.clear();
        for(int i=0; i<1000; i++) {
            pb.write(i%10, ByteBuffer.allocate(4).putInt(i).array());
        }
        pb.close();
    }



}
