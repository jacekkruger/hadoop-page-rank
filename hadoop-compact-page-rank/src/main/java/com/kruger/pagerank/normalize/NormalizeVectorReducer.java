package com.kruger.pagerank.normalize;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.kruger.pagerank.PageRank;

public class NormalizeVectorReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(NormalizeVectorReducer.class);

	double sum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path vectorPath = new Path(conf.get(PageRank.DATA_PATH),
				"vector" + conf.get(PageRank.ITERATION_CURRENT) + "sum/part-r-00000");
		BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(vectorPath)));
		sum = Double.parseDouble(r.readLine().split("\t")[1]);

		if (sum < 0.000001) {
			throw new IllegalArgumentException("Cannot perform normalization - vector is too close to zero");
		}

		log.info("Loaded vector sum " + sum);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double value = StreamSupport.stream(values.spliterator(), false).map(Text::toString)
				.mapToDouble(Double::parseDouble).sum();

		double normalizedValue = value / sum;

		String valueout = String.valueOf(normalizedValue);

		log.info("Emmiting (" + key + ") -> (" + valueout + ")");

		context.write(key, new Text(valueout));
	}
}
