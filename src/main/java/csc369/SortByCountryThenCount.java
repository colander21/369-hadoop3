package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;


public class SortByCountryThenCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryUrlPair, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            String country = arr[0].trim();
            String url = arr[1].trim();
            int count = Integer.parseInt(arr[2].trim());
            context.write(new CountryUrlPair(country, url, count), new Text(url + "\t" + count));
        }
    }

    public static class PartitionerImpl extends Partitioner<CountryUrlPair, Text> {
        @Override
        public int getPartition(CountryUrlPair pair, Text v, int n) {
            return Math.abs(pair.getCountry().hashCode() % n);
        }
    }

    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(CountryUrlPair.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryUrlPair pair = (CountryUrlPair) wc1;
            CountryUrlPair pair2 = (CountryUrlPair) wc2;
            return pair.getCountry().compareTo(pair2.getCountry());
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(CountryUrlPair.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryUrlPair pair = (CountryUrlPair) wc1;
            CountryUrlPair pair2 = (CountryUrlPair) wc2;
            return pair.compareTo(pair2);
        }
    }


    public static class ReducerImpl extends Reducer<CountryUrlPair, Text, Text, IntWritable> {

        @Override
        protected void reduce(CountryUrlPair key,
                              Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String country = key.getCountry().toString();
            for (Text t : values) {
                String[] arr = t.toString().split("\t");
                String url = arr[0].trim();
                int c = Integer.parseInt(arr[1].trim());
                context.write(new Text(country + "\t" + url), new IntWritable(c));
            }
        }
    }
}
