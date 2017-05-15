package com.kruger.pagerank.sumvector;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SumVectorElementsMapper extends Mapper<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(SumVectorElementsReducer.class);

	private static final Text KEY = new Text("sum");

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		log.info("Emmiting (" + KEY + ") -> (" + value + ")");

		context.write(KEY, value);
	}
}
