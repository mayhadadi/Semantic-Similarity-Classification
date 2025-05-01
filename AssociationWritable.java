import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AssociationWritable implements Writable {
    private double rawFrequency;
    private double mle;
    private double pmi;
    private double ttest;

    // Default constructor required for Writable
    public AssociationWritable() {}

    public AssociationWritable(double rawFrequency, double mle, double pmi, double ttest) {
        this.rawFrequency = rawFrequency;
        this.mle = mle;
        this.pmi = pmi;
        this.ttest = ttest;
    }

    public double getRawFrequency() {
        return rawFrequency;
    }

    public double getMle() {
        return mle;
    }

    public double getPmi() {
        return pmi;
    }

    public double getTtest() {
        return ttest;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(rawFrequency);
        out.writeDouble(mle);
        out.writeDouble(pmi);
        out.writeDouble(ttest);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rawFrequency = in.readDouble();
        mle = in.readDouble();
        pmi = in.readDouble();
        ttest = in.readDouble();
    }

    @Override
    public String toString() {
        return rawFrequency + "\t" + mle + "\t" + pmi + "\t" + ttest;
    }
}
