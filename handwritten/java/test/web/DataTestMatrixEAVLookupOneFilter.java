package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixEAVLookupOneFilter {
	static DataTestMatrixEAV DataTestMatrixEAV = new DataTestMatrixEAV();

	@Test
	public void init() throws Exception {
		// TODO: Make Selenium download te Molgenis Matrix export file.
		DataTestMatrixEAV.file = "/pheno_one_filter.txt";
		DataTestMatrixEAV.investigation = "50";
		DataTestMatrixEAV.filter1Table = "BEZOEK1";
		DataTestMatrixEAV.filter1Column = "GEWICHT";
		DataTestMatrixEAV.filter1Operator = ">";
		DataTestMatrixEAV.filter1Value = "100";

		DataTestMatrixEAV.filter1 = true;
		if (DataTestMatrixEAV.parseMatrixColumns() == false)
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "init" })
	public void testMakeGlobalTable() throws Exception {
		DataTestMatrixEAV.makeGlobalTable();
	}

	@Test(dependsOnMethods = { "testMakeGlobalTable" })
	public void testFillGlobalTable() throws Exception {
		DataTestMatrixEAV.fillGlobalTable();
	}
	
	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testCompareGlobalTableToEAV() throws Exception {
		if (DataTestMatrixEAV.compareGlobalTableToEAV() == false)
			Assert.assertFalse(true);
	}
	
}
