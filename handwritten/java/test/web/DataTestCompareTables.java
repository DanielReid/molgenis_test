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
	String metadataQueryOracle;

	String dbDriverMSSQL;
	String dbUrlMSSQL;

	String[] excludedTables;
	String[] excludedColumns;

	Connection connOracle;
	Statement stmtOracle;
	ResultSet rsetOracle;

	Connection connMSSQL;
	Statement stmtMSSQL;
	ResultSet rsetMSSQL;

	Integer stid;
	Integer counterFailLimit;
	Boolean debug = false;
	Integer debugRows = 5;
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
		rsetOracle = stmtOracle.executeQuery(metadataQueryOracle);
		while (rsetOracle.next()) {
			// Check if the table or column is excluded.
			Boolean TableOrColumnNotExcluded = true;
			for (int i = 0; i < excludedTables.length; i++) {
				if (excludedTables[i].toUpperCase().equals(
						rsetOracle.getString(1).toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			for (int i = 0; i < excludedColumns.length; i++) {
				if (excludedColumns[i].toUpperCase().equals(
						rsetOracle.getString(2).toUpperCase()))
					TableOrColumnNotExcluded = false;
			}
			if (TableOrColumnNotExcluded) {
				// Put table and columns relations in a global variable
				// publishUmcgTablesColumns
				if (publishUmcgTablesColumns.get(rsetOracle.getString(1)) == null) {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns.add(rsetOracle.getString(2).toUpperCase());
					publishUmcgTablesColumns.put(rsetOracle.getString(1)
							.toUpperCase(), dbColumns);
				} else {
					ArrayList<String> dbColumns = new ArrayList<String>();
					dbColumns = publishUmcgTablesColumns.get(rsetOracle
							.getString(1).toUpperCase());
					if (dbColumns
							.contains(rsetOracle.getString(2).toUpperCase()) == false)
						dbColumns.add(rsetOracle.getString(2).toUpperCase());
					publishUmcgTablesColumns.put(rsetOracle.getString(1)
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
		rsetOracle = stmtOracle.executeQuery(metadataQueryOracle);
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
		for (Map.Entry<String, ArrayList<String>> tableColumsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			String tab = tableColumsUmcg.getKey();
			for (String col : tableColumsUmcg.getValue()) {
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
		for (Map.Entry<String, ArrayList<String>> tableColumsCit : publishCitTablesColumns
				.entrySet()) {
			String tab = tableColumsCit.getKey();
			for (String col : tableColumsCit.getValue()) {
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
		for (Map.Entry<String, ArrayList<String>> tableColumsUmcg : publishUmcgTablesColumns
				.entrySet()) {
			String sql = "select count(*) from " + tableColumsUmcg.getKey();
			rsetMSSQL = stmtMSSQL.executeQuery(sql);
			rsetMSSQL.next();
			rsetOracle = stmtOracle.executeQuery(sql);
			rsetOracle.next();
			if (Integer.parseInt(rsetMSSQL.getString(1)) == Integer
					.parseInt(rsetOracle.getString(1))) {
				System.out.println("Rowcount " + tableColumsUmcg.getKey()
						+ " both: '" + rsetMSSQL.getString(1) + "'.");
			} else {
				fail = true;
				System.out.println("FAILED: Rowcount "
						+ tableColumsUmcg.getKey() + " CIT: "
						+ rsetOracle.getString(1) + ", UMCG: "
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
		for (Map.Entry<String, ArrayList<String>> tableColums : publishUmcgTablesColumns
				.entrySet()) {
			System.out.println("Processing: " + tableColums);
			// Select a table from UMCG publish.
			String sqlSelectDataset = "";
			for (String col : tableColums.getValue())
				if (sqlSelectDataset.length() == 0) {
					sqlSelectDataset += "select ";
					if (debug)
						sqlSelectDataset += "top " + debugRows + " ";
					sqlSelectDataset += col + " ";
				} else
					sqlSelectDataset += ", " + col + " ";
			sqlSelectDataset += "from " + tableColums.getKey() + " ";
			rsetMSSQL = stmtMSSQL.executeQuery(sqlSelectDataset);
			// Loop UMCG publish table rows.
			while (rsetMSSQL.next()) {
				counter++;
				counterTotal++;
				// Switch between parameter input or 'IS NULL'.
				String sqlPrepareStatement = createSqlPrepareStatementOracle(
						tableColums, rsetMSSQL);
				PreparedStatement psOracle = connOracle
						.prepareStatement(sqlPrepareStatement);
				Integer psIndex = 0;
				// Switch data types and input data.
				for (String col : tableColums.getValue()) {
					if (!(rsetMSSQL.getString(col) == null || rsetMSSQL
							.getString(col).isEmpty())) {
						psIndex++;
						psOracle = createPreparedStatement(rsetMSSQL, psOracle,
								sqlPrepareStatement, col, psIndex);
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
					String sqlPsInvestigate = createSqlPrepareStatementMSSQL(
							tableColums, rsetMSSQL);
					PreparedStatement psInvestigate = connMSSQL
							.prepareStatement(sqlPsInvestigate);
					psIndex = 0;
					for (String col : tableColums.getValue()) {
						if (rsetMSSQL.getString(col) != null) {
							psIndex++;
							psInvestigate = createPreparedStatement(rsetMSSQL,
									psInvestigate, sqlPrepareStatement, col,
									psIndex);
						}
					}
					ResultSet rsetOracleInvestigate = psInvestigate
							.executeQuery();
					rsetOracleInvestigate.next();
					Integer rowsInvestigate = rsetOracleInvestigate.getInt(1);
					rsetOracleInvestigate.close();
					psInvestigate.close();
					if (rowsInvestigate != rows) {
						fail = true;
						failCurrentRow = true;
						counterFail++;
						// Reconstruction for user output.
						System.out
								.println("FAILED: This duplicate row has a different rowcount in Publish Oracle Umcg than in Publish MSSQL CIT: "
										+ sqlReconstruction(tableColums,
												rsetMSSQL));
					}
				}
				if (failCurrentRow) {
					// Reconstruction for user output.
					System.out.println("FAILED:"
							+ " lookup in Publish Oracle CIT: "
							+ sqlReconstruction(tableColums, rsetMSSQL));
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
		for (Map.Entry<String, ArrayList<String>> tableColums : publishCitTablesColumns
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
					System.out.println("FAILED: lookup in Publish MSSQL Umcg: "
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
								.println("FAILED: This duplicate row has a different rowcount in Publish MSSQL Umcg than in Publish Oracle CIT: "
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
			ps.setInt(psIndex, rsetMSSQL.getInt(col));
		else if (datatype == "tinyint" || datatype == "smallint")
			ps.setShort(psIndex, rsetMSSQL.getShort(col));
		else if (datatype == "uniqueidentifier")
			ps.setString(psIndex, "{" + rsetMSSQL.getString(col) + "}");
		else if (datatype == "bigint")
			ps.setLong(psIndex, rsetMSSQL.getLong(col));
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
