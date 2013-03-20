package com.bigdata.elasticsearch.service;

import java.io.File;

public interface DataLoader {
	public void loadData(File dataFile);

	public void getContributionsByCandName(String candName, Double amtEqGtThan);
}
