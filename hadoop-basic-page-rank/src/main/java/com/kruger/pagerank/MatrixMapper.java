package com.kruger.pagerank;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MatrixMapper extends Mapper<Text, Text, Text, Text> {

	protected static final Logger log = Logger.getLogger(MatrixMapper.class);

	protected OutputSize outputSize;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		outputSize = new OutputSize(context.getConfiguration().get(PageRank.OUTPUT_SIZE));
	}

	@Override
	protected void map(Text keyin, Text valuein, Context context) throws IOException, InterruptedException {
		log.info("KEY: " + keyin);
		MatrixElementCoord coord = new MatrixElementCoord(keyin);
		double value = Double.parseDouble(valuein.toString());

		if (coord.getSide() == MatrixSide.LEFT) {
			for (BigInteger i = BigInteger.ONE; i.compareTo(outputSize.getColumns()) <= 0; i = i.add(BigInteger.ONE)) {
				write(context, keyin, valuein, new MatrixElementCoord(MatrixSide.RIGHT, coord.getRow(), i),
						new IntermediateElement(coord.getSide(), coord.getColumn(), value));
			}
		} else {
			for (BigInteger i = BigInteger.ONE; i.compareTo(outputSize.getRows()) <= 0; i = i.add(BigInteger.ONE)) {
				write(context, keyin, valuein, new MatrixElementCoord(MatrixSide.RIGHT, i, coord.getColumn()),
						new IntermediateElement(coord.getSide(), coord.getRow(), value));
			}
		}
	}

	protected void write(Context context, Text keyin, Text valuein, MatrixElementCoord keyout,
			IntermediateElement valueout) throws IOException, InterruptedException {
		log.info("Mapping (" + keyin + ", " + valuein + ") -> (" + keyout + ", " + valueout + ")");

		context.write(keyout.toText(), valueout.toText());
	}

}
