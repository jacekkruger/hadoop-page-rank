package com.kruger.pagerank.normalize;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.kruger.pagerank.PageRank;

public class NormalizeVectorReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(NormalizeVectorReducer.class);

	private double beta;
	private long nodes;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		beta = context.getConfiguration().getDouble(PageRank.BETA, 0.8);
		nodes = context.getConfiguration().getLong(PageRank.NODES, 1);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double sum = StreamSupport.stream(values.spliterator(), false).map(Text::toString)
				.mapToDouble(Double::parseDouble).sum();

		// book, page 192-193
		// That is, when there are dead ends, the sum of the components of v may
		// be less than 1, but it will never reach 0
		double value = (beta * sum) + ((1 - beta) / nodes);

		String valueout = String.valueOf(value);

		log.info("Emmiting (" + key + ") -> (" + valueout + ")");

		context.write(key, new Text(valueout));
	}
}
