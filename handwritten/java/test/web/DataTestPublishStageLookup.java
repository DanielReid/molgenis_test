package test.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.*;

public class DataTestPublishStageLookup {

	// Oracle parameters
	String dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
	String databaseOracle = "llpacc";
	String dbUrlOracle = "jdbc:oracle:thin:@//localhost:2000/" + databaseOracle;
	String dbUsernameOracle = "molgenis3";

	// MSSQL parameters
	String serverMSSQL = "WTZKH0077";
	String databaseMSSQL = "LLCDR_Stage";
	String dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	String dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL + ";databaseName="
			+ databaseMSSQL + ";integratedSecurity=true";

	// Exclusion parameters
	String[] excludedTables = new String[] { "VW_BEZOEK_PIVOT" , "VW_LABDATA_PIVOT" };
	String[] excludedColumns = new String[] { "PA_ID" };

	String metadataQueryOracle = "select tabnaam, veld from vw_dict group by tabnaam, veld";
	Integer counterFailLimit = 999999999;
	Boolean debug = false;
	Integer debugRows = 5;
	Connection connOracle;
	Statement stmtOracle;
	ResultSet rsetOracle;
	Connection connMSSQL;
	Statement stmtMSSQL;
	ResultSet rsetMSSQL;
	Integer totalRecordCount = 0;
	Map<String, ArrayList<String>> publishTablesColumns = new HashMap<String, ArrayList<String>>();

	@Test
	public void testInit() throws Exception {
		init();
		getPublishTablesColumns();
		if (getTotalRecordCount()) {
			Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testCompareTableColums() throws Exception {
		if (compareTableColums())
			Assert.assertFalse(true);
	}

	@Test(dependsOnMethods = { "testCompareTableColums" })
	public void testCompareData() throws Exception {
		if (lookupPublishStage())
			Assert.assertFalse(true);
	}

	public void init() throws Exception {
		Locale.setDefault(Locale.US);
		System.out.println("MSSQL Server: " + serverMSSQL);
		System.out.println("MSSQL Database: " + databaseMSSQL);
		System.out.println("Oracle Database: " + databaseOracle);
		System.out.println("Oracle Username: " + dbUsernameOracle);
		System.out.print("Enter Oracle password: ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String dbPasswordOracle = null;
		dbPasswordOracle = br.readLine();
		try {
			Class.forName(dbDriverOracle);
			connOracle = DriverManager.getConnection(dbUrlOracle,
					dbUsernameOracle, dbPasswordOracle);
			stmtOracle = connOracle.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		try {
			Class.forName(dbDriverMSSQL);
			connMSSQL = DriverManager.getConnection(dbUrlMSSQL);
			stmtMSSQL = connMSSQL.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		// Print out excluded table and columns for the test log.
		if (excludedTables.length != 0)
			System.out.println("WARNING EXCLUDED TABLES: "
					+ Arrays.toString(excludedTables));
		if (excludedColumns.length != 0)
			System.out.println("WARNING EXCLUDED COLUMNS: "
					+ Arrays.toString(excludedColumns));
	}

	public void getPublishTablesColumns() throws Exception {
		System.out
				.println("Getting all the related tables and colums form metadata...");
		rsetOracle = stmtOracle.executeQuery(metadataQueryOracle);
		while (rsetOracle.next()) {
			Boolean TableOrColumnNotExcluded = true;
			String table = rsetOracle.getString(1).toUpperCase();
			String column = rsetOracle.getString(2).toUpperCase();
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].equals(table))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].equals(column))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				if (publishTablesColumns.get(table) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(column);
					publishTablesColumns.put(table, dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishTablesColumns.get(table);
					if (dbColumns.contains(column) == false)
						dbColumns.add(column);
					publishTablesColumns.put(table, dbColumns);
				}
			}
		}
	}

	public boolean getTotalRecordCount() throws Exception {
		boolean fail = false;
		System.out.println("Getting total record count...");
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			try {
				rsetOracle = stmtOracle.executeQuery("select count(*) from "
						+ tabCols.getKey());
				rsetOracle.next();
				totalRecordCount += Integer.parseInt(rsetOracle.getString(1));
			} catch (SQLException e) {
				fail = true;
				System.out.println("select count(*) from " + tabCols.getKey());
				System.out.println(e);
			}
		}
		return fail;
	}

	public Boolean compareTableColums() throws Exception {
		System.out
				.println("Starting table and column lookup in the Stage database...");
		Boolean fail = false;
		Map<String, ArrayList<String>> notInStage = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> foundInStage = new HashMap<String, ArrayList<String>>();
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			for (String col : tabCols.getValue()) {
				String sql = "select count(*) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '"
						+ tabCols.getKey()
						+ "' and COLUMN_NAME = '"
						+ col
						+ "'";
				rsetMSSQL = stmtMSSQL.executeQuery(sql);
				rsetMSSQL.next();
				if (Integer.parseInt(rsetMSSQL.getString(1)) == 1) {
					if (foundInStage.get(tabCols.getKey()) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns.add(col);
						foundInStage.put(tabCols.getKey(), dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = foundInStage.get(tabCols.getKey());
						if (dbColumns.contains(col) == false)
							dbColumns.add(col);
						foundInStage.put(tabCols.getKey(), dbColumns);
					}
				} else {
					fail = true;
					if (notInStage.get(tabCols.getKey()) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns.add(col);
						notInStage.put(tabCols.getKey(), dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = notInStage.get(tabCols.getKey());
						if (dbColumns.contains(col) == false)
							dbColumns.add(col);
						notInStage.put(tabCols.getKey(), dbColumns);
					}
				}

			}
		}
		if (fail)
			System.out
					.println("FAILED: Not found in Stage database MSSQL UMCG: "
							+ notInStage);
		else
			System.out.println("SUCCESS table column selection: "
					+ foundInStage);
		return fail;

	}

	public Boolean compareData() throws Exception {
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		String failMessage = "";
		for (Map.Entry<String, ArrayList<String>> tabCols : publishTablesColumns
				.entrySet()) {
			System.out.println("Starting with: " + tabCols);
			String sqlSelectDataset = "";
			for (String col : tabCols.getValue())
				if (sqlSelectDataset.length() == 0)
					sqlSelectDataset = "select " + col + " ";
				else
					sqlSelectDataset += ", " + col + " ";
			sqlSelectDataset += "from " + tabCols.getKey() + " ";
			rsetOracle = stmtOracle.executeQuery(sqlSelectDataset);
			while (rsetOracle.next()) {
				counter++;
				counterTotal++;
				String sql = "";
				for (String col : tabCols.getValue()) {
					if (sql.length() == 0)
						sql = "select count(*) from " + tabCols.getKey()
								+ " where ";
					else
						sql += "and ";
					if (rsetOracle.getString(col) == null)
						sql += "(" + col + " IS NULL or len(" + col + ") = 0) ";
					else
						sql += col + " = '" + rsetOracle.getString(col) + "' ";
				}

				System.out.println(sql);

				rsetMSSQL = stmtMSSQL.executeQuery(sql);
				rsetMSSQL.next();
				if (rsetMSSQL.getString(1).equals("0")) {
					System.out.println("FAILED: " + sql);
					fail = true;
					failMessage = "PREVIOUSLY FAILED: ";
					counterFail++;
					if (counterFail >= counterFailLimit) {
						System.out.println("FAIL COUNT IS >= "
								+ counterFailLimit + ", ENDING TEST...");
						return true;
					}
				}
				if (counter >= 500) {
					counter = 0;
					System.out.println(failMessage + counterTotal + "/"
							+ totalRecordCount + " records processed.");
				}
				rsetMSSQL.close();
			}
			rsetOracle.close();
		}
		return fail;
	}

	public boolean lookupPublishStage() throws Exception {
		System.out.println("Starting with Publish lookup in Stage.");
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		for (Map.Entry<String, ArrayList<String>> tableColums : publishTablesColumns
				.entrySet()) {
			System.out.println("Processing: " + tableColums);
			String sqlSelectDataset = "";
			for (String col : tableColums.getValue())
				if (sqlSelectDataset.length() == 0)
					sqlSelectDataset = "select " + col + " ";
				else
					sqlSelectDataset += ", " + col + " ";
			sqlSelectDataset += "from " + tableColums.getKey() + " ";
			if (debug)
				sqlSelectDataset += "where rownum <= " + debugRows + " ";
			rsetOracle = stmtOracle.executeQuery(sqlSelectDataset);
			while (rsetOracle.next()) {
				counter++;
				counterTotal++;
				String sqlPrepareStatement = createSqlPrepareStatementMSSQL(
						tableColums, rsetOracle);
				PreparedStatement psMSSQL = connMSSQL
						.prepareStatement(sqlPrepareStatement);
				Integer psIndex = 0;
				for (String col : tableColums.getValue()) {
					if (rsetOracle.getString(col) != null) {
						psIndex++;
						psMSSQL = createPreparedStatement(rsetOracle, psMSSQL,
								sqlPrepareStatement, col, psIndex);
					}
				}
				rsetMSSQL = psMSSQL.executeQuery();
				rsetMSSQL.next();
				Integer rows = rsetMSSQL.getInt(1);
				rsetMSSQL.close();
				psMSSQL.close();
				Boolean failCurrentRow = false;
				if (rows < 1) {
					fail = true;
					failCurrentRow = true;
					counterFail++;
					// Reconstruction for user output.
					System.out.println("FAILED: lookup in Stage "
							+ sqlReconstruction(tableColums, rsetOracle));
				}
				if (rows > 1) {
					// Investigate duplicate rows.
					String sqlPsInvestigate = createSqlPrepareStatementOracle(
							tableColums, rsetOracle);
					PreparedStatement psInvestigate = connOracle
							.prepareStatement(sqlPsInvestigate);
					psIndex = 0;
					for (String col : tableColums.getValue()) {
						if (rsetOracle.getString(col) != null) {
							psIndex++;
							psInvestigate = createPreparedStatement(rsetOracle,
									psInvestigate, sqlPrepareStatement, col,
									psIndex);
						}
					}
					ResultSet rsetMSSQLInvestigate = psInvestigate
							.executeQuery();
					rsetMSSQLInvestigate.next();
					Integer rowsInvestigate = rsetMSSQLInvestigate.getInt(1);
					rsetMSSQLInvestigate.close();
					psInvestigate.close();
					if (rowsInvestigate != rows) {
						fail = true;
						failCurrentRow = true;
						counterFail++;
						// Reconstruction for user output.
						System.out
								.println("FAILED: This duplicate row has a different rowcount in Publish than in Stage: "
										+ sqlReconstruction(tableColums,
												rsetOracle));
					}
				}
				if (failCurrentRow) {
					if (counterFail >= counterFailLimit) {
						System.out.println("FAIL COUNT IS >= "
								+ counterFailLimit + ", ENDING TEST...");
						return true;
					}
				}
				if (counter >= 500) {
					counter = 0;
					System.out.println(counterTotal + "/" + totalRecordCount
							+ " records processed.");
				}
			}
			rsetOracle.close();
		}
		return fail;
	}

	public String createSqlPrepareStatementOracle(
			Map.Entry<String, ArrayList<String>> tableColums, ResultSet rset)
			throws SQLException {
		String sqlPrepareStatement = "";
		for (String col : tableColums.getValue()) {
			if (sqlPrepareStatement.length() == 0)
				sqlPrepareStatement = "select count(*) from "
						+ tableColums.getKey() + " where ";
			else
				sqlPrepareStatement += "and ";
			if (rset.getString(col) == null || rset.getString(col).isEmpty())
				sqlPrepareStatement += col + " is null ";
			else
				sqlPrepareStatement += col + " = ? ";
		}
		return sqlPrepareStatement;
	}

	public String createSqlPrepareStatementMSSQL(
			Map.Entry<String, ArrayList<String>> tableColums, ResultSet rset)
			throws SQLException {
		String sqlPrepareStatement = "";
		for (String col : tableColums.getValue()) {
			if (sqlPrepareStatement.length() == 0)
				sqlPrepareStatement = "select count(*) from "
						+ tableColums.getKey() + " where ";
			else
				sqlPrepareStatement += "and ";
			if (rset.getString(col) == null)
				sqlPrepareStatement += "(" + col + " is null or len(" + col
						+ ") = 0) ";
			else
				sqlPrepareStatement += col + " = ? ";
		}
		return sqlPrepareStatement;
	}

	public PreparedStatement createPreparedStatement(ResultSet rset,
			PreparedStatement ps, String sql, String col, Integer psIndex)
			throws Exception {
		String datatype = rset.getMetaData().getColumnTypeName(
				rset.findColumn(col));
		if (datatype == "NUMBER" || datatype == "numeric"
				|| datatype == "decimal")
			ps.setBigDecimal(psIndex, rset.getBigDecimal(col));
		else if (datatype == "DATE" || datatype == "datetime")
			ps.setTimestamp(psIndex, rset.getTimestamp(col));
		else if (datatype == "VARCHAR2" || datatype == "NVARCHAR2"
				|| datatype == "varchar" || datatype == "nvarchar")
			ps.setString(psIndex, rset.getString(col));
		else if (datatype == "int")
			ps.setInt(psIndex, rset.getInt(col));
		else if (datatype == "tinyint" || datatype == "smallint")
			ps.setShort(psIndex, rset.getShort(col));
		else if (datatype == "uniqueidentifier")
			ps.setString(psIndex, "{" + rset.getString(col) + "}");
		else if (datatype == "bigint")
			ps.setLong(psIndex, rset.getLong(col));
		else {
			ps.setString(psIndex, rset.getString(col));
			System.out.println("DATATYPE CONVERSION FOR'" + datatype
					+ "' MUST BE ADDED TO THIS TEST");
		}
		return ps;
	}

	public String sqlReconstruction(
			Map.Entry<String, ArrayList<String>> tableColums, ResultSet rset)
			throws SQLException {
		String sqlReconstruction = "";
		for (String col : tableColums.getValue()) {
			if (sqlReconstruction.length() == 0)
				sqlReconstruction = "select * from " + tableColums.getKey()
						+ " where ";
			else
				sqlReconstruction += "and ";
			if (rset.getString(col) == null)
				sqlReconstruction += "(" + col + " is null or len(" + col
						+ ") = 0) ";
			else
				sqlReconstruction += col + " = '" + rset.getString(col) + "' ";
		}
		return sqlReconstruction;
	}

}
