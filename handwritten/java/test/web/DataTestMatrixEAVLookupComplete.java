package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixEAVLookupComplete {
	static DataTestMatrixEAV DataTestMatrixEAV = new DataTestMatrixEAV();

	@Test
	public void init() throws Exception {
		// TODO: Make Selenium download te Molgenis Matrix export file.

		DataTestMatrixEAV.investigation = "50";
		DataTestMatrixEAV.file = "/pheno_complete.txt";

		DataTestMatrixEAV.testTablePrefix = "T2_";
		DataTestMatrixEAV.sourceTablePrefix = "LL_";
		DataTestMatrixEAV.matrixSeperator = "\t";
		DataTestMatrixEAV.testOwner = "MOLGENIS";
		
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
