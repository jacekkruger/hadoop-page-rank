package com.kruger.pagerank;

import java.math.BigInteger;

import org.apache.hadoop.io.Text;

public class IntermediateElement {
	private final MatrixSide side;
	private final BigInteger number;
	private final double value;

	public IntermediateElement(Text line) {
		this(line.toString());
	}

	public IntermediateElement(String line) {
		String split[] = line.split(",", 3);
		side = MatrixSide.getByTag(split[0]);
		number = new BigInteger(split[1]);
		value = Double.parseDouble(split[2]);
	}

	public IntermediateElement(MatrixSide side, BigInteger number, double value) {
		this.side = side;
		this.number = number;
		this.value = value;
	}

	public MatrixSide getSide() {
		return side;
	}

	public BigInteger getNumber() {
		return number;
	}

	public double getValue() {
		return value;
	}

	@Override
	public String toString() {
		return new StringBuilder(side.getTag()).append(',').append(number).append(',').append(value).toString();
	}

	public Text toText() {
		return new Text(toString());
	}
}
