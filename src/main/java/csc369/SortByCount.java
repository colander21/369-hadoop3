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


public class SortByCount {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            String country = arr[0].trim();
            int count = Integer.parseInt(arr[1].trim());
            context.write(new IntWritable(count), new Text(country));
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(IntWritable count, Iterable<Text> countries,
                              Context context) throws IOException, InterruptedException {
            for (Text country : countries)
                context.write(count, country);
        }
    }

    public static class DescIntComparator extends WritableComparator {
        protected DescIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

}
