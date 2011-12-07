package test.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/*
 ************************************************************************
 *
 * Package: DataTestCompareTables
 * Purpose: Compare all (but excluded) in given SQL and Oracle databases.
 *
 ************************************************************************
 */
public class DataTestCompareTables {

	String studie;

	String dbDriverOracle;
	String dbUrlOracle;
	String dbUsernameOracle;
	String dbPasswordOracle;
	String sqlQueryOracle;

	String dbDriverMSSQL;
	String dbUrlMSSQL;
	String sqlQueryMSSQL;

	String[] excludedTables;
	String[] excludedColumns;

	Connection connOracle;
	Statement stmtOracle;
	ResultSet rsetOracle;
	PreparedStatement psOracle;

	Connection connMSSQL;
	Statement stmtMSSQL;
	ResultSet rsetMSSQL;
	PreparedStatement psMSSQL;

	Integer stid;
	Integer counterFailLimit;
	Integer debugRows;
	Boolean debug;
	Integer totalRecordCount = 0;

	Map<String, ArrayList<String>> publishUmcgTablesColumns = new HashMap<String, ArrayList<String>>();
	Map<String, ArrayList<String>> publishCitTablesColumns = new HashMap<String, ArrayList<String>>();

	/*
	 * ***********************************************************************
	 * 
	 * Method : init() Purpose: Initialize test. Create database connections.
	 * Give a warning for excluded tables required for the log file.
	 * 
	 * ***********************************************************************
	 */
	public void init() {
		// Set regional setting to US.
		Locale.setDefault(Locale.US);
		// Make Oracle connection.
		try {
			Class.forName(dbDriverOracle);
			connOracle = DriverManager.getConnection(dbUrlOracle,
					dbUsernameOracle, dbPasswordOracle);
			stmtOracle = connOracle.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		// Make SQL connection.
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
		if (debug)
			System.out.println("DEBUG MODE " + debugRows + " ROWS/TABLE ");
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : getPublishUmcgTablesColumns() Purpose: Get all UMCG/Publish
	 * tables and columns.
	 * 
	 * ***********************************************************************
	 */
	public void getPublishUmcgTablesColumns() throws Exception {
		// Select all tables and columns from the UMCG Publish database
		rsetMSSQL = stmtMSSQL.executeQuery(sqlQueryMSSQL);
		while (rsetMSSQL.next()) {
			// Check if the table or column is excluded.
			Boolean TableOrColumnNotExcluded = true;
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].toUpperCase().equals(
						rsetMSSQL.getString(1).toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].toUpperCase().equals(
						rsetMSSQL.getString(2).toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				// Put table and columns relations in a global variable
				// publishUmcgTablesColumns
				if (publishUmcgTablesColumns.get(rsetMSSQL.getString(1)) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(rsetMSSQL.getString(2).toUpperCase());
					publishUmcgTablesColumns.put(rsetMSSQL.getString(1)
							.toUpperCase(), dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishUmcgTablesColumns.get(rsetMSSQL
							.getString(1).toUpperCase());
					if (dbColumns
							.contains(rsetMSSQL.getString(2).toUpperCase()) == false)
						dbColumns.add(rsetMSSQL.getString(2).toUpperCase());
					publishUmcgTablesColumns.put(rsetMSSQL.getString(1)
							.toUpperCase(), dbColumns);
				}
			}
		}
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : getPublishCitTablesColumns() Purpose: Get all CIT/Publish tables
	 * and columns.
	 * 
	 * ***********************************************************************
	 */
	public void getPublishCitTablesColumns() throws Exception {
		// Select all tables and columns from the UMCG Publish database
		rsetOracle = stmtOracle.executeQuery(sqlQueryOracle);
		while (rsetOracle.next()) {
			// Check if the table or column is excluded.
			Boolean TableOrColumnNotExcluded = true;
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].equals(rsetOracle.getString(1)
						.toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].equals(rsetOracle.getString(2)
						.toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				// Put table and columns relations in a global variable
				// publishCitTablesColumns
				if (publishCitTablesColumns.get(rsetOracle.getString(1)) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(rsetOracle.getString(2).toUpperCase());
					publishCitTablesColumns.put(rsetOracle.getString(1)
							.toUpperCase(), dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishCitTablesColumns.get(rsetOracle
							.getString(1).toUpperCase());
					if (dbColumns.contains(rsetOracle.getString(2)
							.toUpperCase()) == false)
						dbColumns.add(rsetOracle.getString(2).toUpperCase());
					publishCitTablesColumns.put(rsetOracle.getString(1)
							.toUpperCase(), dbColumns);
				}
			}
		}
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : compareTableColumns() Purpose: Compare the table and column
	 * structure from both Publish databases
	 * 
	 * ***********************************************************************
	 */
	public boolean compareTableColumns() {
		Boolean fail = false;
		Map<String, ArrayList<String>> notInCit = new HashMap<String, ArrayList<String>>();
		Map<String, ArrayList<String>> notInUmcg = new HashMap<String, ArrayList<String>>();
		// Lookup missing publishUmcgTablesColumns in publishCitTablesColumns
		// and put the optional results in notInCit.
		for (Map.Entry<String, ArrayList<String>> tabColsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			String tab = tabColsUmcg.getKey();
			for (String col : tabColsUmcg.getValue()) {
				try {
					if (publishCitTablesColumns.get(tab).contains(col) == false)
						throw new Exception();
				} catch (Exception e) {
					fail = true;

					if (notInCit.get(tab) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns.add(col);
						notInCit.put(tab, dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = notInCit.get(tab);
						if (dbColumns.contains(col) == false)
							dbColumns.add(col);
						notInCit.put(tab, dbColumns);
					}

				}
			}
		}
		// Lookup missing publishCitTablesColumns in publishUmcgTablesColumns
		// and put the optional results in notInCit.
		for (Map.Entry<String, ArrayList<String>> tabColsCit : publishCitTablesColumns
				.entrySet()) {
			String tab = tabColsCit.getKey();
			for (String col : tabColsCit.getValue()) {
				try {
					if (publishUmcgTablesColumns.get(tab).contains(col) == false)
						throw new Exception();
				} catch (Exception e) {
					fail = true;
					if (notInUmcg.get(tab) == null) {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns.add(col);
						notInUmcg.put(tab, dbColumns);
					} else {
						ArrayList<String> dbColumns = new ArrayList<String>();
						dbColumns = notInUmcg.get(tab);
						if (dbColumns.contains(col) == false)
							dbColumns.add(col);
						notInUmcg.put(tab, dbColumns);
					}
				}
			}
		}
		// If there are table or columns notInCit give output and fail.
		if (fail) {
			if (notInCit.size() >= 1)
				System.out
						.println("FAILED: Not found in Publish database Oracle CIT: "
								+ notInCit);
			if (notInUmcg.size() >= 1)
				System.out
						.println("FAILED: Not found in Publish database MSSQL UMCG: "
								+ notInUmcg);
		} else
			System.out.println("SUCCESS table column selection: "
					+ publishUmcgTablesColumns);
		return fail;
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : rowCountUmcgVersusCit() Purpose: Compare row count Publish
	 * databases. Get a total row count of all tables.
	 * 
	 * 
	 * ***********************************************************************
	 */
	public boolean rowCountUmcgVersusCit() throws Exception {
		System.out.println("Starting rowcount...");
		Boolean fail = false;
		for (Map.Entry<String, ArrayList<String>> tabColsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			String sql = "select count(*) from " + tabColsUmcg.getKey();
			rsetMSSQL = stmtMSSQL.executeQuery(sql);
			rsetMSSQL.next();
			rsetOracle = stmtOracle.executeQuery(sql);
			rsetOracle.next();
			if (Integer.parseInt(rsetMSSQL.getString(1)) == Integer
					.parseInt(rsetOracle.getString(1))) {
				System.out.println("Rowcount " + tabColsUmcg.getKey()
						+ " both: '" + rsetMSSQL.getString(1) + "'.");
			} else {
				fail = true;
				System.out.println("FAILED: Rowcount " + tabColsUmcg.getKey()
						+ " CIT: " + rsetOracle.getString(1) + ", UMCG: "
						+ rsetMSSQL.getString(1) + ".");

			}
			totalRecordCount += Integer.parseInt(rsetMSSQL.getString(1));
			rsetMSSQL.close();
			rsetOracle.close();
		}
		return fail;
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : lookupDataUmcgInCit() Purpose: Lookup all table rows from
	 * Publish UMCG in Publish CIT.
	 * 
	 * ***********************************************************************
	 */
	public boolean lookupDataUmcgInCit() throws Exception {
		System.out
				.println("Starting with Publish MSSQL UMCG lookup in Publish Oracle CIT.");
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		// Loop table columns
		for (Map.Entry<String, ArrayList<String>> tabCols : publishUmcgTablesColumns
				.entrySet()) {
			System.out.println("Processing: " + tabCols);
			// Select a table from UMCG publish.
			String sql = "";
			String selectCol = "";
			sql += "select ";
			if (debug)
				sql += "top " + debugRows + " ";
			sql += "\n";
			for (String col : tabCols.getValue()) {
				if (selectCol.length() == 0)
					selectCol += "  ";
				else
					selectCol += "  ,";
				selectCol += col + "\n";
			}
			sql += selectCol;
			sql += "from\n";
			sql += "  " + tabCols.getKey() + "\n";
			rsetMSSQL = stmtMSSQL.executeQuery(sql);
			// Loop UMCG publish table rows.
			while (rsetMSSQL.next()) {
				counter++;
				counterTotal++;
				String whereColVal = "";
				// Switch between parameter input or 'IS NULL'.
				sql = "select count(*) from " + tabCols.getKey() + " where ";
				for (String col : tabCols.getValue()) {
					if (whereColVal.length() != 0)
						whereColVal += "and ";
					if (rsetMSSQL.getString(col) == null
							|| rsetMSSQL.getString(col).isEmpty())
						whereColVal += col + " is null ";
					else
						whereColVal += col + " = ? ";
				}
				sql += whereColVal;
				psOracle = connOracle.prepareStatement(sql);
				Integer psIndex = 0;
				// Switch data types and input data.
				for (String col : tabCols.getValue()) {
					if (!(rsetMSSQL.getString(col) == null || rsetMSSQL
							.getString(col).isEmpty())) {
						psIndex++;
						String datatype = rsetMSSQL.getMetaData()
								.getColumnTypeName(rsetMSSQL.findColumn(col));
						if (datatype == "int")
							psOracle.setInt(psIndex, rsetMSSQL.getInt(col));
						else if (datatype == "smallint")
							psOracle.setShort(psIndex, rsetMSSQL.getShort(col));
						else if (datatype == "tinyint")
							psOracle.setShort(psIndex, rsetMSSQL.getShort(col));
						else if (datatype == "numeric")
							psOracle.setBigDecimal(psIndex,
									rsetMSSQL.getBigDecimal(col));
						else if (datatype == "decimal")
							psOracle.setBigDecimal(psIndex,
									rsetMSSQL.getBigDecimal(col));
						else if (datatype == "datetime")
							psOracle.setTimestamp(psIndex,
									rsetMSSQL.getTimestamp(col));
						else if (datatype == "varchar")
							psOracle.setString(psIndex,
									rsetMSSQL.getString(col));
						else if (datatype == "nvarchar")
							psOracle.setString(psIndex,
									rsetMSSQL.getString(col));
						else if (datatype == "uniqueidentifier")
							psOracle.setString(psIndex,
									"{" + rsetMSSQL.getString(col) + "}");
						else if (datatype == "bigint")
							psOracle.setLong(psIndex, rsetMSSQL.getLong(col));
						else {
							psOracle.setString(psIndex,
									rsetMSSQL.getString(col));
							System.out
									.println("DATATYPE CONVERSION FOR '"
											+ datatype
											+ "' MUST BE ADDED TO THIS TEST");
						}
					}
				}
				// Lookup row in CIT Publish.
				rsetOracle = psOracle.executeQuery();
				rsetOracle.next();
				Integer rows = rsetOracle.getInt(1);
				rsetOracle.close();
				psOracle.close();

				Boolean failCurrentRow = false;
				if (rows < 1) {
					fail = true;
					failCurrentRow = true;
					counterFail++;
				}
				if (rows > 1) {
					// Investigate duplicate rows.
				}
				if (failCurrentRow) {
					// Reconstruction for user output.
					sql = "select * from " + tabCols.getKey() + " where ";
					whereColVal = "";
					for (String col : tabCols.getValue()) {
						if (whereColVal.length() != 0)
							whereColVal += "and ";
						if (rsetMSSQL.getString(col) == null)
							whereColVal += col + " is null ";
						else
							whereColVal += col + " = '"
									+ rsetMSSQL.getString(col) + "' ";
					}
					sql += whereColVal;
					System.out.println("FAILED:"
							+ " lookup in Publish Oracle CIT: " + sql);
					if (counterFail >= counterFailLimit) {
						System.out.println("FAILED: FAIL COUNT IS >= "
								+ counterFailLimit + ", ENDING TEST...");
						return true;
					}
				}
				// Give feedback every 500 rows.
				if (counter >= 500) {
					counter = 0;
					System.out
							.println(counterTotal
									+ "/"
									+ totalRecordCount
									+ " records processed (1/2 Publish MSSQL UMCG lookup in Publish Oracle CIT).");
				}
			}
			rsetMSSQL.close();
		}
		return fail;
	}

	/*
	 * ***********************************************************************
	 * 
	 * Method : lookupDataUmcgInCit() Purpose: Lookup all table rows from
	 * Publish CIT in Publish UMCG.
	 * 
	 * ***********************************************************************
	 */
	public boolean lookupDataCitInUmcg() throws Exception {
		System.out
				.println("Starting with Publish Oracle Cit lookup in Publish MSSQL Umcg.");
		Boolean fail = false;
		Integer counter = 0;
		Integer counterTotal = 0;
		Integer counterFail = 0;
		for (Map.Entry<String, ArrayList<String>> tabCols : publishCitTablesColumns
				.entrySet()) {
			System.out.println("Processing: " + tabCols);
			String sql = "";
			String selectCol = "";
			sql += "select\n";
			for (String col : tabCols.getValue()) {
				if (selectCol.length() == 0)
					selectCol += "  ";
				else
					selectCol += "  ,";
				selectCol += col + "\n";
			}
			sql += selectCol;
			sql += "from\n";
			sql += "  " + tabCols.getKey() + "\n";
			if (debug)
				sql += "where rownum <= " + debugRows + " ";
			rsetOracle = stmtOracle.executeQuery(sql);
			while (rsetOracle.next()) {
				counter++;
				counterTotal++;
				String whereColVal = "";
				sql = "select count(*) from " + tabCols.getKey() + " where ";
				for (String col : tabCols.getValue()) {
					if (whereColVal.length() != 0)
						whereColVal += "and ";
					if (rsetOracle.getString(col) == null)
						whereColVal += "(" + col + " is null or len(" + col
								+ ") = 0) ";
					else
						whereColVal += col + " = ? ";
				}
				sql += whereColVal;
				psMSSQL = connMSSQL.prepareStatement(sql);
				Integer psIndex = 0;
				for (String col : tabCols.getValue()) {
					if (rsetOracle.getString(col) != null) {
						psIndex++;
						String datatype = rsetOracle.getMetaData()
								.getColumnTypeName(rsetOracle.findColumn(col));
						if (datatype == "NUMBER")
							psMSSQL.setBigDecimal(psIndex,
									rsetOracle.getBigDecimal(col));
						else if (datatype == "DATE")
							psMSSQL.setTimestamp(psIndex,
									rsetOracle.getTimestamp(col));
						else if (datatype == "VARCHAR2")
							psMSSQL.setString(psIndex,
									rsetOracle.getString(col));
						else if (datatype == "NVARCHAR2")
							psMSSQL.setString(psIndex,
									rsetOracle.getString(col));
						else {
							psMSSQL.setString(psIndex,
									rsetOracle.getString(col));
							System.out
									.println("DATATYPE CONVERSION FOR'"
											+ datatype
											+ "' MUST BE ADDED TO THIS TEST");
						}
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
				}
				if (rows > 1) {
					// Investigate duplicate rows.
				}
				if (failCurrentRow) {
					// Reconstruction for user output.
					sql = "select * from " + tabCols.getKey() + " where ";
					whereColVal = "";
					for (String col : tabCols.getValue()) {
						if (whereColVal.length() != 0)
							whereColVal += "and ";
						whereColVal += col + " ";
						if (rsetOracle.getString(col) == null)
							whereColVal += "(" + col + " is null or len(" + col
									+ ") = 0) ";
						else
							whereColVal += " = '" + rsetOracle.getString(col)
									+ "' ";
					}
					sql += whereColVal;
					System.out.println("FAILED: lookup in Publish MSSQL Umcg: "
							+ sql);
					if (counterFail >= counterFailLimit) {
						System.out.println("FAIL COUNT IS >= "
								+ counterFailLimit + ", ENDING TEST...");
						return true;
					}
				}
				if (counter >= 500) {
					counter = 0;
					System.out
							.println(counterTotal
									+ "/"
									+ totalRecordCount
									+ " records processed (2/2 Publish Oracle Cit lookup in Publish MSSQL Umcg).");
				}
			}
			rsetOracle.close();
		}
		return fail;
	}
}
