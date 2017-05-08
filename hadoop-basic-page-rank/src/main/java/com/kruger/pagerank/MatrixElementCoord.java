package com.kruger.pagerank;

import java.math.BigInteger;

import org.apache.hadoop.io.Text;

public class MatrixElementCoord {
	private final MatrixSide side;
	private final BigInteger row;
	private final BigInteger column;

	public MatrixElementCoord(Text line) {
		this(line.toString());
	}

	public MatrixElementCoord(String line) {
		String split[] = line.split(",", 3);
		side = MatrixSide.getByTag(split[0]);
		row = new BigInteger(split[1]);
		column = new BigInteger(split[2]);
	}

	public MatrixElementCoord(MatrixSide side, BigInteger row, BigInteger column) {
		this.side = side;
		this.row = row;
		this.column = column;
	}

	public MatrixSide getSide() {
		return side;
	}

	public BigInteger getRow() {
		return row;
	}

	public BigInteger getColumn() {
		return column;
	}

	@Override
	public String toString() {
		return new StringBuilder(side.getTag()).append(',').append(row).append(',').append(column).toString();
	}

	public Text toText() {
		return new Text(this.toString());
	}
}
