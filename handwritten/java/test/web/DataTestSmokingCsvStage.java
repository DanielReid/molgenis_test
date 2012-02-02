package test.web;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.testng.Assert;
import org.testng.annotations.Test;
import au.com.bytecode.opencsv.CSVReader;

public class DataTestSmokingCsvStage {

	// Input CSV file
	String csvFile = "H:/Data/Smoking/smoking.csv";

	// MSSQL parameters
	String serverMSSQL = "WTZKH0077";
	String databaseMSSQL = "LLCDR_Stage";
	String dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	String dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL + ";databaseName="
			+ databaseMSSQL + ";integratedSecurity=true";

	Connection connMSSQL;
	Statement stmtMSSQL;
	ResultSet rsetMSSQL;

	CSVReader reader;
	String sql;
	List<String> columnsIndex = new ArrayList<String>();
	List<String[]> csvData = new ArrayList<String[]>();
	List<String[]> dbData = new ArrayList<String[]>();

	@Test
	public void testInit() throws Exception {
		connectDatabase();
		getColumnsIndex();
		getCsvData();
		getDbData();
		compare();
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testCompare() throws Exception {
		if (false) {
			Assert.assertFalse(true);
		}
	}

	public void connectDatabase() {
		// Set regional setting to US.
		Locale.setDefault(Locale.US);

		// Make SQL connection.
		try {
			Class.forName(dbDriverMSSQL);
			connMSSQL = DriverManager.getConnection(dbUrlMSSQL);
			stmtMSSQL = connMSSQL.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getColumnsIndex() throws Exception {
		System.out.println("Getting column index...");
		reader = new CSVReader(new InputStreamReader(new FileInputStream(
				csvFile), "UTF-8"));
		String[] columns = reader.readNext();
		for (String column : columns)
			columnsIndex.add(column);
	}

	public void getCsvData() throws Exception {
		csvData = reader.readAll();
		reader.close();
	}

	public void getDbData() throws Exception {
		sql = "select ";
		for (String column : columnsIndex)
			sql += column + ", ";
		sql = sql.substring(0, sql.length() - 2) + " from smoking";
		rsetMSSQL = stmtMSSQL.executeQuery(sql);
		while (rsetMSSQL.next()) {
			List<String> lRow = new ArrayList<String>();
			for (int i = 1; i <= columnsIndex.size(); i++) {
				if (rsetMSSQL.getString(i) == null)
					lRow.add("");
				else
					lRow.add(rsetMSSQL.getString(i));
			}
			String[] saRow = lRow.toArray(new String[lRow.size()]);
			dbData.add(saRow);
		}
	}

	public void compare() throws Exception {
		boolean fail = false;
		int csvRowCount = csvData.size();
		int dbRowCount = dbData.size();
		if (csvRowCount != dbRowCount) {
			fail = true;
			System.out.println("FAIL: rowcount CSV file: " + csvRowCount
					+ " database: " + dbRowCount);
			System.out.println();
		} else {
			System.out.println("Rowcount: " + csvRowCount);
			System.out.println();
		}
		// Lookup CSV in Database
		int totalCounter = 0;
		int stepCounter = 0;
		System.out.println("Lookup CSV in Database [" + totalCounter + "/"
				+ csvRowCount + "].");
		for (String[] csvLine : csvData) {
			totalCounter++;
			stepCounter++;
			if (stepCounter >= 500) {
				stepCounter = 0;
				System.out.println("Lookup CSV in Database [" + totalCounter
						+ "/" + csvRowCount + "].");
				System.out.println();
			}
			int intAmountInDb = amountInDb(csvLine);
			if (intAmountInDb > 1) {
				// Look back
				int intAmountInCsv = amountInCsv(csvLine);
				if (intAmountInDb != intAmountInCsv) {
					fail = true;
					System.out.println("FAIL: Count db/csv: " + intAmountInDb
							+ "/" + intAmountInCsv + ".");
					System.out.println("CSV Line: " + getLine(csvLine));
					System.out.println("SQL: " + getSqlLine(csvLine));
					System.out.println();
				}
			}
			if (intAmountInDb == 0) {
				fail = true;
				System.out.println("FAIL: Not found in database.");
				System.out.println("CSV Line: " + getLine(csvLine));
				System.out.println("SQL: " + getSqlLine(csvLine));
				System.out.println();
			}
		}
		// Lookup Database in CVS
		totalCounter = 0;
		stepCounter = 0;
		System.out.println("Lookup Database in CVS [" + totalCounter + "/"
				+ dbRowCount + "].");
		for (String[] dbLine : dbData) {
			totalCounter++;
			stepCounter++;
			if (stepCounter >= 500) {
				stepCounter = 0;
				System.out.println("Lookup Database in CVS [" + totalCounter
						+ "/" + dbRowCount + "].");
				System.out.println();
			}
			int intAmountInCsv = amountInCsv(dbLine);
			if (intAmountInCsv > 1) {
				// Look back
				int intAmountInDb = amountInDb(dbLine);
				if (intAmountInCsv != intAmountInDb) {
					fail = true;					
					System.out.println("FAIL: Count db/csv: " + intAmountInDb
							+ "/" + intAmountInCsv + ".");
					System.out.println("CSV Line: " + getLine(dbLine));
					System.out.println("SQL: " + getSqlLine(dbLine));
					System.out.println();				
				}
			}
			if (intAmountInCsv == 0) {
				fail = true;
				System.out.println("FAIL: Not found in CSV.");
				System.out.println("CSV Line: " + getLine(dbLine));
				System.out.println("SQL: " + getSqlLine(dbLine));
				System.out.println();
			}
		}

	}

	public int amountInDb(String[] line) {
		int maches = 0;
		for (String[] dbLine : dbData) {
			if (Arrays.equals(line, dbLine))
				maches++;
		}
		return maches;
	}

	public int amountInCsv(String[] line) {
		int maches = 0;
		for (String[] csvLine : csvData) {
			if (Arrays.equals(line, csvLine))
				maches++;
		}
		return maches;
	}

	public String getLine(String[] line) {
		String strLine = "";
		for (int i = 0; i < line.length; i++)
			strLine += line[i] + ",";
		return strLine.substring(0, strLine.length() - 1);
	}

	public String getSqlLine(String[] line) {
		String sql1 = sql + " where ";
		for (int i = 0; i < line.length; i++) {
			// Handle null values
			if (line[i].equals("") || line[i].toLowerCase().equals("null"))
				sql1 += columnsIndex.get(i) + " is null and ";
			// Handle numeric values
			else if (line[i].matches("[-+]?\\d+(\\.\\d+)?"))
				sql1 += columnsIndex.get(i) + " = " + line[i] + " and ";
			// Handle string values
			else
				sql1 += columnsIndex.get(i) + " = '"
						+ line[i].replace("'", "''") + "' and ";
		}
		return sql1.substring(0, sql1.length() - 4);
	}

}
