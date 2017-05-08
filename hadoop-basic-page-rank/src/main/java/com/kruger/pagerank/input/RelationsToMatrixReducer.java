package com.kruger.pagerank.input;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.kruger.pagerank.MatrixElementCoord;
import com.kruger.pagerank.MatrixSide;

public class RelationsToMatrixReducer extends Reducer<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(RelationsToMatrixReducer.class);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		BigInteger from = new BigInteger(key.toString());
		List<BigInteger> tos = StreamSupport.stream(values.spliterator(), false).map(Text::toString)
				.map(BigInteger::new).collect(Collectors.toList());

		if (tos.isEmpty()) {
			return;
		}

		double value = 1.0d / tos.size();
		Text valueout = new Text(String.valueOf(value));

		for (BigInteger to : tos) {
			Text keyout = new MatrixElementCoord(MatrixSide.LEFT, to, from).toText();
			context.write(keyout, valueout);
		}
	}
}
