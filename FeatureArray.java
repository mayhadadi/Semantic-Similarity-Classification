

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

public class FeatureArray {
    private static class singletonHolder {
        private static FeatureArray instance = null;
    }
        private int[] occurances;
        private String[] features;

    public static FeatureArray getInstance() {
        if(singletonHolder.instance==null)
            singletonHolder.instance = new FeatureArray();
        return singletonHolder.instance;
    }

    private FeatureArray() {
        
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        
            S3Object getObjectResponse = s3.getObject("ass3-corpus-bucket", "top1000.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(getObjectResponse.getObjectContent()));
        
            String line;
            int i=0;
        
            try {
                while ((line = reader.readLine()) != null) {
                features[i]=(line.split("\t")[0]);
                occurances[i]=Integer.parseInt(line.split("\t")[1]);
        }
    } catch (IOException ex) {
        ex.printStackTrace();
    }
    }


    protected String[] getFeatures(){
        return features;
    }

    protected int[] getoccurances(){
        return occurances;
    }

    

}