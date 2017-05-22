package com.kruger.pagerank.normalize;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.kruger.pagerank.PageRank;

public class NormalizeVectorReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(NormalizeVectorReducer.class);

	private double beta;
	private Set<String> topics = new HashSet<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		beta = context.getConfiguration().getDouble(PageRank.BETA, 0.8);

		for (String topic : context.getConfiguration().get(PageRank.TOPICS).split(",")) {
			topics.add(topic);
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double sum = StreamSupport.stream(values.spliterator(), false).map(Text::toString)
				.mapToDouble(Double::parseDouble).sum();

		// book, page 192-193
		double value = (beta * sum);
		if (topics.contains(key.toString())) {
			value += (1 - beta) / topics.size();
		}

		String valueout = String.valueOf(value);

		log.debug("Emmiting (" + key + ") -> (" + valueout + ")");

		context.write(key, new Text(valueout));
	}
}
