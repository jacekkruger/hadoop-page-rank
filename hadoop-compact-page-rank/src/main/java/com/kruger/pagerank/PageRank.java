package com.kruger.pagerank;

import java.io.IOException;
import java.util.Objects;
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
import org.apache.hadoop.util.GenericOptionsParser;

import com.kruger.pagerank.input.RelationsToCompactJob;
import com.kruger.pagerank.normalize.NormalizeVectorJob;

public class PageRank {

	public static final String DATA_PATH = "pagerank.data.path";
	public static final String NODES = "pagerank.nodes";
	public static final String BETA = "pagerank.beta";
	public static final String ITERATION_COUNT = "pagerank.iteration.count";
	public static final String ITERATION_PREVIOUS = "pagerank.iteration.previous";
	public static final String ITERATION_CURRENT = "pagerank.iteration.current";

	public static final String REQUIRED_PARAMS[] = { DATA_PATH, NODES, BETA, ITERATION_COUNT };

	public static void main(String[] args) throws Exception {
		Configuration conf = createConfigurationAndValidate(args, REQUIRED_PARAMS);

		Job relationsToMatrixJob = RelationsToCompactJob.createJob(conf);
		runAssertSuccess(relationsToMatrixJob, () -> "Failed to prepare matrix");

		int iterations = Integer.parseInt(conf.get(ITERATION_COUNT));

		for (int i = 0; i < iterations; i++) {
			conf.set(ITERATION_PREVIOUS, String.valueOf(i));
			conf.set(ITERATION_CURRENT, String.valueOf(i + 1));
			Job coreJob = createJob(conf);
			runAssertSuccess(coreJob, () -> "Core job failed. Iteration " + conf.get(ITERATION_CURRENT));
			Job normalizeJob = NormalizeVectorJob.createJob(conf);
			runAssertSuccess(normalizeJob, () -> "Normalize job failed. Iteration " + conf.get(ITERATION_CURRENT));
		}
	}

	public static Configuration createConfigurationAndValidate(String[] args, String[] requiredProps)
			throws IOException {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);

		for (String param : requiredProps) {
			Objects.requireNonNull(conf.get(param), "Parameter " + param + " is required");
		}

		return conf;
	}

	private static void runAssertSuccess(Job job, Supplier<String> errMessage)
			throws IOException, InterruptedException, ClassNotFoundException {
		if (!job.waitForCompletion(true)) {
			throw new RuntimeException(errMessage.get());
		}
	}

	private static Job createJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, "Matrix × Vector");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(MatrixReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(conf.get(PageRank.DATA_PATH), "matrix"));
		FileInputFormat.addInputPath(job,
				new Path(conf.get(PageRank.DATA_PATH), "vector" + conf.get(PageRank.ITERATION_PREVIOUS)));
		FileOutputFormat.setOutputPath(job,
				new Path(conf.get(PageRank.DATA_PATH), "vector" + conf.get(PageRank.ITERATION_CURRENT) + "part"));

		return job;
	}
}
