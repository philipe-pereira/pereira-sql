package br.com.pereiraeng.sql;

import br.com.pereiraeng.core.MemoryManager;

public class SQLmemoryManager implements MemoryManager {

	private double percMax;

	private double mbMax;

	public SQLmemoryManager() {
		this(300.);
	}

	public SQLmemoryManager(double mbMax) {
		this(90., mbMax);
	}

	public SQLmemoryManager(double percMax, double mbMax) {
		this.percMax = percMax;
		this.mbMax = mbMax;
	}

	@Override
	public boolean isMoreMemoryNeeded(Runtime runtime) {
		if (runtime == null)
			runtime = Runtime.getRuntime();
		long tm = runtime.totalMemory(), fm = runtime.freeMemory();
		double perc = 100. * (tm - fm) / (double) tm;
		return perc > percMax || ((tm - fm) / 1E6) > mbMax;
	}
}
