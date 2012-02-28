package test.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestLifelinesStageLookup {

	// MSSQL parameters
	String server = "WTZKH0077";
	String databaseLifelines = "LLCDR_Stage";
	String databaseStage = "LLCDR_Stage";
	String tableColumnQuery = "select c.table_name, c.column_name from information_schema.tables t join information_schema.columns c on t.table_name = c.table_name where t.table_type = 'VIEW' and c.table_name like 'VW_TARGET_%' and c.table_name not like '%_HULP'";

	String databaseDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	String urlLifelines = "jdbc:sqlserver://" + server + ";databaseName="
			+ databaseLifelines + ";integratedSecurity=true";
	Connection connLifelines;
	Statement stmtLifelines;
	Statement stmtLifelines2;
	ResultSet rsetLifelines;
	ResultSet rsetLifelines2;
	String urlStage = "jdbc:sqlserver://" + server + ";databaseName="
			+ databaseStage + ";integratedSecurity=true";
	Connection connStage;
	Statement stmtStage;
	Statement stmtStage2;
	ResultSet rsetStage;
	ResultSet rsetStage2;

	int totalLifelines = 0;
	int totalStage = 0;
	Map<String, ArrayList<String>> tablesColumns = new HashMap<String, ArrayList<String>>();

	@Test
	public void testInit() throws Exception {
		connectDatabases();
		getTablesColumns();
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testTotalRecordCount() throws Exception {
		if (totalRecordCount()) {
			Assert.assertFalse(true);
		}
	}

	@Test(dependsOnMethods = { "testTotalRecordCount" })
	public void testCompare() throws Exception {
		if (compare()) {
			Assert.assertFalse(true);
		}
	}

	public void connectDatabases() {
		// Set regional setting to US.
		Locale.setDefault(Locale.US);

		// Make SQL connections
		try {
			Class.forName(databaseDriver);
			connLifelines = DriverManager.getConnection(urlLifelines);
			stmtLifelines = connLifelines.createStatement();
			stmtLifelines2 = connLifelines.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
		try {
			Class.forName(databaseDriver);
			connStage = DriverManager.getConnection(urlStage);
			stmtStage = connStage.createStatement();
			stmtStage2 = connStage.createStatement();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getTablesColumns() throws Exception {
		System.out.println("Getting table columns...");
		rsetLifelines = stmtLifelines.executeQuery(tableColumnQuery);
		while (rsetLifelines.next()) {
			String table = rsetLifelines.getString(1).toUpperCase();
			String column = rsetLifelines.getString(2).toUpperCase();
			if (tablesColumns.get(table) == null) {
				ArrayList<String> dbColumns = new ArrayList<String>();
				dbColumns.add(column);
				tablesColumns.put(table, dbColumns);
			} else {
				ArrayList<String> dbColumns = new ArrayList<String>();
				dbColumns = tablesColumns.get(table);
				if (dbColumns.contains(column) == false)
					dbColumns.add(column);
				tablesColumns.put(table, dbColumns);
			}
		}
		System.out.println(tablesColumns);
	}

	public Boolean totalRecordCount() throws Exception {
		Boolean fail = false;
		System.out.println("Getting total record count...");
		for (String table : tablesColumns.keySet()) {
			rsetLifelines = stmtLifelines.executeQuery("select count(*) from "
					+ table);
			rsetLifelines.next();
			totalLifelines += Integer.parseInt(rsetLifelines.getString(1));
		}
		for (String table : tablesColumns.keySet()) {
			rsetStage = stmtStage.executeQuery("select count(*) from "
					+ table.replace("VW_TARGET_", ""));
			rsetStage.next();
			totalStage += Integer.parseInt(rsetStage.getString(1));
		}
		if (totalLifelines == totalStage)
			System.out.println("Total: " + totalLifelines);
		else {
			fail = true;
			System.out.println("FAIL rowcount Lifelines: " + totalLifelines
					+ ", Stage: " + totalStage + ".");
		}
		return fail;
	}

	public Boolean compare() throws Exception {
		Boolean fail = false;
		System.out
				.println("Starting lookup Lifelines in the Stage database...");
		int totalCounter = 0;
		int stepCounter = 0;
		for (Map.Entry<String, ArrayList<String>> tableColumns : tablesColumns
				.entrySet()) {
			System.out.println("Lookup table: " + tableColumns.getKey());
			String sql = "select ";
			for (String column : tableColumns.getValue())
				sql += column + ", ";
			sql = sql.substring(0, sql.length() - 2) + " from "
					+ tableColumns.getKey();
			rsetLifelines = stmtLifelines.executeQuery(sql);
			while (rsetLifelines.next()) {
				totalCounter++;
				stepCounter++;
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("Lookup Lifelines in Stage database ["
							+ totalCounter + "/" + totalLifelines + "].");
				}
				String sqlSelectLifelines = "select count(*) from "
						+ tableColumns.getKey() + " ";
				String sqlSelectStage = "select count(*) from "
						+ tableColumns.getKey().replace("VW_TARGET_", "") + " ";
				String sqlWhere = "where ";
				for (String column : tableColumns.getValue()) {
					// Handle null values
					if (rsetLifelines.getString(column) == null)
						sqlWhere += column + " is null and ";
					else
						sqlWhere += column
								+ " = '"
								+ rsetLifelines.getString(column).replace("'",
										"''") + "' and ";
				}
				sqlWhere = sqlWhere.substring(0, sqlWhere.length() - 4);
				rsetStage = stmtStage.executeQuery(sqlSelectStage + sqlWhere);
				rsetStage.next();
				if (Integer.parseInt(rsetStage.getString(1)) == 0) {
					fail = true;
					System.out.println("/* FAIL Not found in Stage database */"
							+ sqlSelectStage + sqlWhere);
				}
				// Investigate duplicate rows
				if (Integer.parseInt(rsetStage.getString(1)) >= 1) {
					rsetLifelines2 = stmtLifelines2
							.executeQuery(sqlSelectLifelines + sqlWhere);
					rsetLifelines2.next();
					if (Integer.parseInt(rsetStage.getString(1)) != Integer
							.parseInt(rsetLifelines2.getString(1))) {
						fail = true;
						System.out
								.println("/* FAIL Other rowcount in Stage database */"
										+ sqlSelectStage + sqlWhere);
					}
				}
			}
		}

		System.out
				.println("Starting lookup Stage in the Lifelines database...");
		totalCounter = 0;
		stepCounter = 0;
		for (Map.Entry<String, ArrayList<String>> tableColumns : tablesColumns
				.entrySet()) {
			System.out.println("Lookup table: " + tableColumns.getKey());
			String sql = "select ";
			for (String column : tableColumns.getValue())
				sql += column + ", ";
			sql = sql.substring(0, sql.length() - 2) + " from "
					+ tableColumns.getKey();
			rsetStage = stmtStage.executeQuery(sql);
			while (rsetStage.next()) {
				totalCounter++;
				stepCounter++;
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("Lookup Stage in Lifelines database ["
							+ totalCounter + "/" + totalStage + "].");
				}
				String sqlSelectLifelines = "select count(*) from "
						+ tableColumns.getKey() + " ";
				String sqlSelectStage = "select count(*) from "
						+ tableColumns.getKey().replace("VW_TARGET_", "") + " ";
				String sqlWhere = "where ";
				for (String column : tableColumns.getValue()) {
					// Handle null values
					if (rsetStage.getString(column) == null)
						sqlWhere += column + " is null and ";
					else
						sqlWhere += column
								+ " = '"
								+ rsetStage.getString(column)
										.replace("'", "''") + "' and ";
				}
				sqlWhere = sqlWhere.substring(0, sqlWhere.length() - 4);
				rsetLifelines = stmtLifelines.executeQuery(sqlSelectLifelines
						+ sqlWhere);
				rsetLifelines.next();
				if (Integer.parseInt(rsetLifelines.getString(1)) == 0) {
					fail = true;
					System.out
							.println("/* FAIL Not found in Lifelines database */"
									+ sqlSelectLifelines + sqlWhere);
				}
				// Investigate duplicate rows
				if (Integer.parseInt(rsetLifelines.getString(1)) >= 1) {
					rsetStage2 = stmtStage2.executeQuery(sqlSelectStage
							+ sqlWhere);
					rsetStage2.next();
					if (Integer.parseInt(rsetLifelines.getString(1)) != Integer
							.parseInt(rsetStage2.getString(1))) {
						fail = true;
						System.out
								.println("/* FAIL Other rowcount in Lifelines database */"
										+ sqlSelectLifelines + sqlWhere);
					}
				}
			}
		}

		return fail;
	}

}
