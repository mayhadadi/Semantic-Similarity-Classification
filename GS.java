import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

public class GS {
    private static class singletonHolder {
        private static GS instance = null;
    }
    protected Set<String> words;
    private HashMap<String, Set<String>> pairs;
    private HashMap<String, Boolean> values;
    private HashMap<String, String> labels; // NEW: Store the original label string ("True"/"False")

    public static GS getInstance() {
        if (singletonHolder.instance == null)
            singletonHolder.instance = new GS();
        return singletonHolder.instance;
    }

    private GS() {
        words = new HashSet<>();
        pairs = new HashMap<>();
        values = new HashMap<>();
        labels = new HashMap<>(); // initialize new labels map
        try {
            // Create an S3 client and fetch the file from the given S3 location.
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                              .withRegion(Regions.US_WEST_2)
                              .build();
            S3Object s3Object = s3.getObject("ass3emb3151", "data/word-relatedness.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            String line = reader.readLine();
            while (line != null) {
                String[] l = line.split("\\s+");
                if (l.length < 3) {
                    line = reader.readLine();
                    continue;
                }
                // Add both words to the set.
                words.add(l[0]);
                words.add(l[1]);
                // Build the pair mapping.
                pairs.putIfAbsent(l[0], new HashSet<String>());
                pairs.get(l[0]).add(l[1]);
                pairs.putIfAbsent(l[1], new HashSet<String>());
                pairs.get(l[1]).add(l[0]);
                // Store boolean value as before.
                boolean val = l[2].equals("True");
                values.putIfAbsent(l[0] + " " + l[1], val);
                // NEW: Also save the original label (e.g., "True" or "False") directly.
                labels.putIfAbsent(l[0] + " " + l[1], l[2].trim());
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected boolean ContainInWords(String w) {
        return words.contains(w);
    }

    protected Set<String> getSetFromPairs(String w) {
        return pairs.getOrDefault(w, null);
    }

    protected String getValue(String w1, String w2) {
        Boolean val;
        if (values.containsKey(w1 + " " + w2)) {
            val = values.get(w1 + " " + w2);
            return val ? "yes" : "no";
        } else if (values.containsKey(w2 + " " + w1)) {
            val = values.get(w2 + " " + w1);
            return val ? "yes" : "no";
        } else {
            return null;
        }
    }
    
    // NEW: This method returns the original label ("True"/"False") from the GS file.
    public String getLabel(String w1, String w2) {
        if (labels.containsKey(w1 + " " + w2)) {
            return labels.get(w1 + " " + w2);
        } else if (labels.containsKey(w2 + " " + w1)) {
            return labels.get(w2 + " " + w1);
        } else {
            return null;
        }
    }
}
