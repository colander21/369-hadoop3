package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;


public class SortByUrlThenCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            String country = arr[0].trim();
            String url = arr[1].trim();
            context.write(new Text(url), new Text(country));
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text url, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeSet<String> uniq = new java.util.TreeSet<>();
            for (Text t : values) {
                uniq.add(t.toString().trim());
            }
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String c : uniq) {
                if (!first) sb.append(", ");
                sb.append(c);
                first = false;
            }
            context.write(url, new Text(sb.toString()));
        }
    }
}
