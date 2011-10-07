package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupTwoFilters {
	static DataTestMatrix DataTestMatrix = new DataTestMatrix();

	@Test
	public void init() throws Exception {
		// TODO: Make Selenium download te Molgenis Matrix export file
		DataTestMatrix.file = "d:/ll_pheno_quantitatief_two_filters_subset.txt";
		DataTestMatrix.getMatrixColumnsIndex();
		if (DataTestMatrix.getDbTablesColumns() == false)
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "init" })
	public void testMakeGlobalTable() throws Exception {
		DataTestMatrix.makeGlobalTable();
	}

	@Test(dependsOnMethods = { "testMakeGlobalTable" })
	public void testFillGlobalTable() throws Exception {
		DataTestMatrix.fillGlobalTable();
	}

	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testMakeTables() throws Exception {
		DataTestMatrix.makeTables();
	}

	@Test(dependsOnMethods = { "testMakeTables" })
	public void testFillTables() throws Exception {
		DataTestMatrix.fillTables();
	}

	@Test(dependsOnMethods = { "testFillTables" })
	public void testCompareTablesTwoFilters() throws Exception {
		if (DataTestMatrix.compareFilteredTables("BEZOEK1", "GEWICHT", "=",
				"50", "PATIENT", "GESLACHT", "=", "2") == false)
			Assert.assertFalse(true);
	}

}