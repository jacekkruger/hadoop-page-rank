package com.kruger.pagerank.normalize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.kruger.pagerank.PageRank;

public class NormalizeVectorJob {

	public static Job createJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Sum vector elements");
		job.setJarByClass(NormalizeVectorJob.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(NormalizeVectorReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job,
				new Path(conf.get(PageRank.DATA_PATH), "vector" + conf.get(PageRank.ITERATION_CURRENT) + "part"));
		FileOutputFormat.setOutputPath(job,
				new Path(conf.get(PageRank.DATA_PATH), "vector" + conf.get(PageRank.ITERATION_CURRENT)));

		return job;
	}
}
