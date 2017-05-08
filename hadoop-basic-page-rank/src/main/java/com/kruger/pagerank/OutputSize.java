package com.kruger.pagerank;

import java.math.BigInteger;

public class OutputSize {
	private final BigInteger rows;
	private final BigInteger columns;

	public OutputSize(String line) {
		String split[] = line.split(",", 2);
		rows = new BigInteger(split[0]);
		columns = new BigInteger(split[1]);
	}

	public BigInteger getRows() {
		return rows;
	}

	public BigInteger getColumns() {
		return columns;
	}
}
