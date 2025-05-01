// MyHashMapWritable.java
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

public class MyHashMapWritable implements Writable {
    private HashMap<String, Long> map;

    public MyHashMapWritable() {
        map = new HashMap<>();
    }

    // Get the underlying map
    public HashMap<String, Long> getMap() {
        return map;
    }

    // Put a key/value pair
    public void put(String key, Long value) {
       // System.out.println("put this key :"+key+"put this value "+value.toString(0));
        map.put(key, value);
    }

    // Get a value by key
    public Long get(String key) {
        return map.get(key);
    }

    // Merge another MyHashMapWritable into this one.
    public void merge(MyHashMapWritable other) {
        for (Map.Entry<String, Long> entry : other.map.entrySet()) {
            String key = entry.getKey();
            Long value = entry.getValue();
            map.put(key, map.getOrDefault(key, 0L) + value);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            long value = in.readLong();
            map.put(key, value);
        }
    }
    
    @Override
    public String toString() {
        return map.toString();
    }
}
