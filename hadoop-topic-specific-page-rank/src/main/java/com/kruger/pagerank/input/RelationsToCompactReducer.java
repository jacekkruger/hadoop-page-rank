package com.kruger.pagerank.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class RelationsToCompactReducer extends Reducer<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(RelationsToCompactReducer.class);

	@Override
	protected void reduce(Text key, Iterable<Text> destinations, Context context)
			throws IOException, InterruptedException {
		long degree = 0;

		StringBuilder valueout = new StringBuilder();

		for (Text destination : destinations) {
			degree++;
			valueout.append(',').append(destination);
		}

		if (degree > 0) {
			valueout.insert(0, degree);
			context.write(key, new Text(valueout.toString()));
		}
	}
}
