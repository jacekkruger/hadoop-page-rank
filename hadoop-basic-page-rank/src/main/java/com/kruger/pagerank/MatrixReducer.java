package com.kruger.pagerank;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {

	private final static Logger log = Logger.getLogger(MatrixReducer.class);

	@Override
	protected void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<MatrixSide, List<IntermediateElement>> sides = StreamSupport.stream(values.spliterator(), false)
				.map(Text::toString).map(IntermediateElement::new).collect(groupingBy(IntermediateElement::getSide));

		Map<BigInteger, Double> leftElements = sides.getOrDefault(MatrixSide.LEFT, emptyList()).stream()
				.collect(groupingBy(IntermediateElement::getNumber,
						mapping(IntermediateElement::getValue, singletonCollector())));

		Map<BigInteger, Double> rightElements = sides.getOrDefault(MatrixSide.RIGHT, emptyList()).stream()
				.collect(groupingBy(IntermediateElement::getNumber,
						mapping(IntermediateElement::getValue, singletonCollector())));

		double sum = 0;
		for (Entry<BigInteger, Double> l : leftElements.entrySet()) {
			for (Entry<BigInteger, Double> r : rightElements.entrySet()) {
				if (l.getKey().equals(r.getKey())) {
					sum = sum + (l.getValue() * r.getValue());
				}
			}
		}

		log.info("Emmiting (" + keyin + ") -> (" + sum + ")");
		context.write(keyin, new Text(String.valueOf(sum)));
	}

	protected void write(Context context, Text keyin, String keyout, String valueout)
			throws IOException, InterruptedException {
		log.info("Emmiting (" + keyin + ") -> (" + keyout + ", " + valueout + ")");

		context.write(new Text(keyout), new Text(valueout));
	}

	static <T> Collector<T, ?, T> singletonCollector() {
		return collectingAndThen(toList(), list -> {
			if (list.size() != 1) {
				throw new IllegalStateException();
			}
			return list.get(0);
		});
	}
}
