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

public class Request3 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

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
            String url = arr[6];
            context.write(new Text(hostname), new Text("B\t"+ url));
        }
    }


    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = null;
            String url = null;
            ArrayList<String> urlList = new ArrayList<>();
            int count = 0;
            for (Text val : values) {
                String s = val.toString();
                if (s.startsWith("A\t")) {
                    country = s.substring(2);
                } else if (s.startsWith("B\t")) {
                    url = s.substring(2);
                    urlList.add(url);
                }
            }
            if (country != null && !urlList.isEmpty()){
                for (String u : urlList) {
                    context.write(new Text(country), new Text(u));
                }
            }
        }
    }
}