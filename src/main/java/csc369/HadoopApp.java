package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
            System.exit(-1);
        }
        else if ("Request1".equalsIgnoreCase(otherArgs[0])){

            Path tmp1 = new Path(otherArgs[3]+"_tmp1");
            Path tmp2 = new Path(otherArgs[3]+"_tmp2");

            Job job1 = new Job(conf, "Request1");
            MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Request1.HostNameCountryMapper.class);
            MultipleInputs.addInputPath(job1, new Path(otherArgs[2]), TextInputFormat.class, Request1.AccessLogMapper.class);

            job1.setReducerClass(Request1.JoinReducer.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Request1.OUTPUT_KEY_CLASS);
            job1.setOutputValueClass(Request1.OUTPUT_VALUE_CLASS);

            FileOutputFormat.setOutputPath(job1, tmp1);

            job1.waitForCompletion(true);

            Job job2 = new Job(conf, "SumByCountry");

            job2.setMapperClass(SumByCountry.MapperImpl.class);
            job2.setReducerClass(SumByCountry.ReducerImpl.class);

            job2.setOutputKeyClass(SumByCountry.OUTPUT_KEY_CLASS);
            job2.setOutputValueClass(SumByCountry.OUTPUT_VALUE_CLASS);

            FileInputFormat.addInputPath(job2, tmp1);
            FileOutputFormat.setOutputPath(job2, tmp2);

            job2.waitForCompletion(true);

            Job job3 = new Job(conf, "SortByCount");

            job3.setMapperClass(SortByCount.MapperImpl.class);
            job3.setReducerClass(SortByCount.ReducerImpl.class);

            job3.setOutputKeyClass(SortByCount.OUTPUT_KEY_CLASS);
            job3.setOutputValueClass(SortByCount.OUTPUT_VALUE_CLASS);
            FileInputFormat.addInputPath(job3, tmp2);
            FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));

            job3.setSortComparatorClass(SortByCount.DescIntComparator.class);

            job3.waitForCompletion(true);
        }
        else if ("Request2".equalsIgnoreCase(otherArgs[0])){

            Path tmp1 = new Path(otherArgs[3]+"_tmp1");
            Path tmp2 = new Path(otherArgs[3]+"_tmp2");

            Job job1 = new Job(conf, "Request2");
            MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Request2.HostNameCountryMapper.class);
            MultipleInputs.addInputPath(job1, new Path(otherArgs[2]), TextInputFormat.class, Request2.AccessLogMapper.class);

            job1.setReducerClass(Request2.JoinReducer.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Request2.OUTPUT_KEY_CLASS);
            job1.setOutputValueClass(Request2.OUTPUT_VALUE_CLASS);

            FileOutputFormat.setOutputPath(job1, tmp1);

            job1.waitForCompletion(true);

            Job job2 = new Job(conf, "SumByCountryURL");

            job2.setMapperClass(SumByCountryURL.MapperImpl.class);
            job2.setReducerClass(SumByCountryURL.ReducerImpl.class);

            job2.setOutputKeyClass(SumByCountryURL.OUTPUT_KEY_CLASS);
            job2.setOutputValueClass(SumByCountryURL.OUTPUT_VALUE_CLASS);

            FileInputFormat.addInputPath(job2, tmp1);
            FileOutputFormat.setOutputPath(job2, tmp2);

            job2.waitForCompletion(true);

            Job job3 = new Job(conf, "SortByCountryThenCount");

            job3.setMapperClass(SortByCountryThenCount.MapperImpl.class);
            job3.setReducerClass(SortByCountryThenCount.ReducerImpl.class);

            job3.setMapOutputKeyClass(CountryUrlPair.class);
            job3.setMapOutputValueClass(Text.class);

            job3.setOutputKeyClass(SortByCountryThenCount.OUTPUT_KEY_CLASS);
            job3.setOutputValueClass(SortByCountryThenCount.OUTPUT_VALUE_CLASS);

            job3.setGroupingComparatorClass(SortByCountryThenCount.GroupingComparator.class);
            job3.setSortComparatorClass(SortByCountryThenCount.SortComparator.class);
            job3.setPartitionerClass(SortByCountryThenCount.PartitionerImpl.class);


            FileInputFormat.addInputPath(job3, tmp2);
            FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));

            job3.setNumReduceTasks(1);

            job3.waitForCompletion(true);
        }
        else if ("Request3".equalsIgnoreCase(otherArgs[0])){

            Path tmp1 = new Path(otherArgs[3]+"_tmp1");

            Job job1 = new Job(conf, "Request3");
            MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, Request2.HostNameCountryMapper.class);
            MultipleInputs.addInputPath(job1, new Path(otherArgs[2]), TextInputFormat.class, Request2.AccessLogMapper.class);

            job1.setReducerClass(Request3.JoinReducer.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Request3.OUTPUT_KEY_CLASS);
            job1.setOutputValueClass(Request3.OUTPUT_VALUE_CLASS);

            FileOutputFormat.setOutputPath(job1, tmp1);

            job1.waitForCompletion(true);

            Job job2 = new Job(conf, "SortByUrlThenCountry");

            job2.setMapperClass(SortByUrlThenCountry.MapperImpl.class);
            job2.setReducerClass(SortByUrlThenCountry.ReducerImpl.class);

            job2.setOutputKeyClass(SortByUrlThenCountry.OUTPUT_KEY_CLASS);
            job2.setOutputValueClass(SortByUrlThenCountry.OUTPUT_VALUE_CLASS);

            FileInputFormat.addInputPath(job2, tmp1);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

            job2.waitForCompletion(true);
        }
        else {
            System.out.println("Unrecognized job: " + otherArgs[0]);
            System.exit(-1);
        }
    }

}
