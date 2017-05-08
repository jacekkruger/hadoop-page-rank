package com.kruger.pagerank.input;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.kruger.pagerank.PageRank;

public class RelationsToMatrixJob {

	public static final String REQUIRED_PARAMS[] = { PageRank.DATA_PATH };

	protected static final Logger log = Logger.getLogger(RelationsToMatrixJob.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = PageRank.createConfigurationAndValidate(args, REQUIRED_PARAMS);

		Job job = createJob(conf);

		runAssertSuccess(job, () -> "Failed to prepare matrix");
	}

	public static Job createJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Relations to stochastic matrix");
		job.setJarByClass(RelationsToMatrixJob.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(RelationsToMatrixReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(conf.get(PageRank.DATA_PATH), "relations"));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(PageRank.DATA_PATH), "matrix"));
		return job;
	}

	private static void runAssertSuccess(Job job, Supplier<String> errMessage)
			throws IOException, InterruptedException, ClassNotFoundException {
		if (!job.waitForCompletion(true)) {
			throw new RuntimeException(errMessage.get());
		}
	}
}
