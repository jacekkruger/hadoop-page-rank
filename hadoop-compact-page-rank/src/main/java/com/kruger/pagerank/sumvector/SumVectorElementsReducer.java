package com.kruger.pagerank.sumvector;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SumVectorElementsReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(SumVectorElementsReducer.class);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double sum = StreamSupport.stream(values.spliterator(), false).map(Text::toString)
				.mapToDouble(Double::parseDouble).sum();

		String valueout = String.valueOf(sum);

		log.info("Emmiting (" + key + ") -> (" + valueout + ")");

		context.write(key, new Text(valueout));
	}
}
