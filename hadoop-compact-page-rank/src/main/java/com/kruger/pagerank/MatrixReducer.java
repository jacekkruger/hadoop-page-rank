package com.kruger.pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(MatrixReducer.class);

	double randomJump;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		randomJump = context.getConfiguration().getDouble(PageRank.RANDOM_JUMP, 0.8);
		log.info("Configured random jump chance to " + randomJump);
	}

	@Override
	protected void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		long degree = 0;
		Set<Long> destinations = new HashSet<>();
		double vectorElement = Double.NaN;

		for (Text textValue : values) {
			String value = textValue.toString();
			String[] splitValue = value.split(",");
			if (splitValue.length > 1) {
				// matrix

				if (degree != 0) {
					throw new IllegalStateException("Multiple matrix elements");
				}

				degree = Long.parseLong(splitValue[0]);
				if (degree == 0) {
					throw new IllegalStateException("Zero degree matrix element");
				}

				for (int i = 1; i < splitValue.length; i++) {
					destinations.add(Long.valueOf(splitValue[i]));
				}
			} else {
				// vector element
				log.info("Processing vector element " + keyin + " with value " + value);

				if (!Double.isNaN(vectorElement)) {
					throw new IllegalStateException("Multiple vector elements");
				}

				vectorElement = Double.parseDouble(value);
			}
		}

		if (vectorElement == Double.NaN) {
			log.info("No vector element for index " + keyin);
			return;
		}

		if (degree == 0) {
			log.info("No matrix element for index " + keyin);
			return;
		}

		double value = randomJump * vectorElement / degree;
		Text valueout = new Text(String.valueOf(value));

		for (Long destination : destinations) {
			Text keyout = new Text(destination.toString());
			log.info("Emmiting (" + keyin + ") -> (" + keyout + ", " + valueout + ")");
			context.write(keyout, valueout);
		}
	}
}
