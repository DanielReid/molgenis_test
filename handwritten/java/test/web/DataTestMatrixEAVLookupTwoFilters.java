package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixEAVLookupTwoFilters {
	static DataTestMatrixEAV DataTestMatrixEAV = new DataTestMatrixEAV();

	@Test
	public void init() throws Exception {
		// TODO: Make Selenium download te Molgenis Matrix export file.
		DataTestMatrixEAV.file = "/pheno_two_filters.txt";
		DataTestMatrixEAV.investigation = "50";
		DataTestMatrixEAV.filter1Table = "BEZOEK1";
		DataTestMatrixEAV.filter1Column = "GEWICHT";
		DataTestMatrixEAV.filter1Operator = ">";
		DataTestMatrixEAV.filter1Value = "100";
		DataTestMatrixEAV.filter2Table = "BEZOEK1";
		DataTestMatrixEAV.filter2Column = "LENGTE";
		DataTestMatrixEAV.filter2Operator = "<";
		DataTestMatrixEAV.filter2Value = "170";

		DataTestMatrixEAV.filter1 = true;
		DataTestMatrixEAV.filter2 = true;
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
