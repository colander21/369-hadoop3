package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class CountryUrlPair
        implements Writable, WritableComparable<CountryUrlPair> {

    private final Text country = new Text();
    private final Text url = new Text();
    private final IntWritable count = new IntWritable();

    public CountryUrlPair() {
    }

    public CountryUrlPair(String country, String url, int count) {
        this.country.set(country);
        this.url.set(url);
        this.count.set(count);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        country.write(out);
        url.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        url.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(CountryUrlPair pair) {
        if (country.compareTo(pair.getCountry()) == 0) {
            if (count.compareTo(pair.count) == 0){
                return url.compareTo(pair.url);
            }
            return -count.compareTo(pair.count);
        }
        return country.compareTo(pair.getCountry());
    }

    public Text getCountry() {
        return country;
    }

    public Text getUrl() {return url;}

    public IntWritable getCount() {
        return count;
    }

}
