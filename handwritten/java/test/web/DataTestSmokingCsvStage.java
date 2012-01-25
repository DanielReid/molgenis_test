package test.web;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Test;
import au.com.bytecode.opencsv.CSVReader;

public class DataTestSmokingCsvStage {

	String csvFile = "d:/smoking.csv";
	String serverMSSQL = "WTZKH0077";
	String databaseMSSQL = "LLCDR_Stage";
	String[] excludedColumns = new String[] { "LL_NR", "WGA_LLNR", "GESLACHT",
			"GEBDAT", "gwa" };
	String dbDriverMSSQL = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	String dbUrlMSSQL = "jdbc:sqlserver://" + serverMSSQL + ";databaseName="
			+ databaseMSSQL + ";integratedSecurity=true";

	ArrayList<String> columns = new ArrayList<String>();
	Connection connMSSQL;
	Statement stmtMSSQL;
	ResultSet rsetMSSQL;

	@Test
	public void testInit() throws Exception {
		System.out.println("WARNING Exluded CSV columns: "
				+ Arrays.asList(excludedColumns));
		connectDatabase();
		getColumnsIndex();
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testCompare() throws Exception {
		if (lookupDataRows()) {
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
		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(csvFile), "UTF-8"));
		String[] headerLine = reader.readNext();
		for (int i = 0; i < headerLine.length; i++) {
			columns.add(headerLine[i]);
		}
	}

	public boolean lookupDataRows() throws Exception {
		boolean fail = false;
		System.out.println("Lookup data rows...");

		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(csvFile), "UTF-8"));
		String[] line;
		// Skip header.
		reader.readNext();
		Integer totalCounter = 1;
		Integer counter = 1;
		while ((line = reader.readNext()) != null) {
			totalCounter++;
			counter++;
			if (counter >= 500) {
				counter = 0;
				System.out.println("Rowcount: " + totalCounter);
			}
			Boolean exception = false;
			String sql = "select count(*) from smoking where ";
			String diagnoseDbSelect = "select 'DATABASE RESULTS:' as SOURCE, ";
			String diagnoseCsvSelect = "select 'CSV RESULTS:' as SOURCE, ";
			String diagnoseId = "";
			for (int i = 0; i < columns.size(); i++) {
				try {
					if (!Arrays.asList(excludedColumns)
							.contains(columns.get(i))) {
						// Handle null values
						if (line[i].equals("")
								|| line[i].toUpperCase().equals("NULL")) {
							sql += columns.get(i) + " is null and ";
							diagnoseCsvSelect += "null, ";
						}
						// Handle numeric values
						else if (line[i].matches("[-+]?\\d+(\\.\\d+)?")) {
							sql += columns.get(i) + " = " + line[i] + " and ";
							diagnoseCsvSelect += line[i] + ", ";
						}
						// Handle string values
						else {
							sql += columns.get(i) + " = '"
									+ line[i].replace("'", "''") + "' and ";
							diagnoseCsvSelect += "'"
									+ line[i].replace("'", "''") + "', ";
						}

						diagnoseDbSelect += columns.get(i) + ", ";
						if (columns.get(i).toUpperCase().equals("ID"))
							diagnoseId += line[i];
					}
				} catch (Exception e) {
					exception = true;
					fail = true;
				}
			}

			diagnoseDbSelect = diagnoseDbSelect.substring(0,
					diagnoseDbSelect.length() - 2)
					+ " from smoking where id = " + diagnoseId;
			diagnoseCsvSelect = diagnoseCsvSelect.substring(0,
					diagnoseCsvSelect.length() - 2);

			if (exception)
				System.out.println("FAIL: Wrong line length. Line:"
						+ Arrays.asList(line));
			else {
				sql = sql.substring(0, sql.length() - 4);
				rsetMSSQL = stmtMSSQL.executeQuery(sql);
				rsetMSSQL.next();
				if (rsetMSSQL.getString(1).equals("0")) {
					fail = true;
					System.out.println("/* FAILED SQL: */ " + sql);
					System.out.println("\t/* Diagnose SQL: */ "
							+ diagnoseDbSelect + " union " + diagnoseCsvSelect);
				}
			}
		}
		return fail;
	}
}
