package test.web;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixEAVLookupTwoFilters {
	static DataTestMatrixEAV DataTestMatrixEAV = new DataTestMatrixEAV();

	@Test
	public void init() throws Exception {

		DataTestMatrixEAV.file = "/pheno_two_filters.txt";
		DataTestMatrixEAV.investigation = "50";

		DataTestMatrixEAV.testTablePrefix = "T2_";
		DataTestMatrixEAV.sourceTablePrefix = "LL_";
		DataTestMatrixEAV.matrixSeperator = "\t";
		DataTestMatrixEAV.testOwner = "MOLGENIS";

		DataTestMatrixEAV.dbDriver = "oracle.jdbc.driver.OracleDriver";
		DataTestMatrixEAV.dbUrl = "jdbc:oracle:thin:@//localhost:2000/llptest";
		DataTestMatrixEAV.dbUsername = "molgenis";
		DataTestMatrixEAV.dbPassword = "molTagtGen24Ora";

		// inverse filters
		String filter1Table = "BEZOEK1";
		String filter1Column = "MEETSTAND";
		String filter1Operator = "<>";
		String filter1Value = "1";
		String filter2Table = "BEZOEK1";
		String filter2Column = "GEWICHT";
		String filter2Operator = "<>";
		String filter2Value = "80";

		DataTestMatrixEAV.caseWhenEndAs = "end as";
		DataTestMatrixEAV.caseWhenCondition = "case when (p.name = '"
				+ DataTestMatrixEAV.sourceTablePrefix + filter1Table
				+ "' and oe.name = '" + filter1Column + "' and ov.value "
				+ filter1Operator + " '" + filter1Value + "') or (p.name = '"
				+ DataTestMatrixEAV.sourceTablePrefix + filter2Table
				+ "' and oe.name = '" + filter2Column + "' and ov.value "
				+ filter2Operator + " '" + filter2Value + "') then null else ";

		DataTestMatrixEAV.init();
	}

	@Test(dependsOnMethods = { "init" })
	public void testparseMatrixColumns() throws Exception {
		if (DataTestMatrixEAV.parseMatrixColumns() == false)
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testparseMatrixColumns" })
	public void testFillGlobalTable() throws Exception {
		DataTestMatrixEAV.makeGlobalTable();
		DataTestMatrixEAV.fillGlobalTable();
	}

	@Test(dependsOnMethods = { "testFillGlobalTable" })
	public void testCompareGlobalTableToEAV() throws Exception {
		if (DataTestMatrixEAV.compareGlobalTableToEAV() == false)
			Assert.assertFalse(true);
	}
}