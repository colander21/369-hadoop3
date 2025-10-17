package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Request1 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class HostNameCountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String arr[] = value.toString().split(",");
            String hostname = arr[0];
            String location = arr[1];
            String newVal = "A\t"+ location;
            context.write(new Text(hostname), new Text(newVal));
        }
    }


    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String arr[] = value.toString().split(" ");
            String hostname = arr[0];
            context.write(new Text(hostname), new Text("B\t1"));
        }
    }


    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = null;
            int count = 0;

            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("A\t")) {
                    country = s.substring(2);
                }
                else if (s.startsWith("B\t")){
                    count++;
                }
            }

            if (count != 0 && country != null)
                context.write(new Text(country), new IntWritable(count));
        }
    }
}