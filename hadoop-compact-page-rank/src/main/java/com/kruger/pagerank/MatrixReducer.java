package com.kruger.pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(MatrixReducer.class);

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
				log.debug("Processing vector element " + keyin + " with value " + value);

				if (!Double.isNaN(vectorElement)) {
					throw new IllegalStateException("Multiple vector elements");
				}

				vectorElement = Double.parseDouble(value);
			}
		}

		if (Double.isNaN(vectorElement)) {
			log.debug("No vector element for index " + keyin);
			return;
		}

		if (degree == 0) {
			log.debug("No matrix element for index " + keyin);
			return;
		}

		double value = vectorElement / degree;
		Text valueout = new Text(String.valueOf(value));

		for (Long destination : destinations) {
			Text keyout = new Text(destination.toString());
			log.debug("Emmiting (" + keyin + ") -> (" + keyout + ", " + valueout + ")");
			context.write(keyout, valueout);
		}
	}
}
