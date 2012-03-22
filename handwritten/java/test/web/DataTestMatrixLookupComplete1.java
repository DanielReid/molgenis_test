package test.web;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataTestMatrixLookupComplete1 {

	String file = "H:/Data/export/Export_All_2012-03-20_11_36_57_molgenis2_llp.csv";
	String databaseOracle = "llp";
	String dbUsernameOracle = "molgenis2";
	String matrixStringDateFormat = "yyyy-mm-dd";
	String dict = "select tabnaam, veld from vw_dict "
			/* remove this part after meta data bug fix */
			+ "minus select tabnaam, veld from vw_dict where tabnaam = 'BEZOEK1' and veld = 'PA_ID'"
			+ "minus select tabnaam, veld from vw_dict where tabnaam = 'ECGLEADS' and veld = 'PA_ID'"
			+ "minus select tabnaam, veld from vw_dict where tabnaam = 'BLOEDDRUKAVG' and veld = 'PA_ID'"
			+ "minus select tabnaam, veld from vw_dict where veld = 'ID'";
	String dictValues = "select tabnaam, veld, vallabelabel, vallabelval from vw_dict_valuesets";
	String[] idFields = { "ID", "PA_ID", "BZ_ID" };

	String sqlSetDateFormat = "alter session set nls_date_format='"
			+ matrixStringDateFormat + "'";
	ArrayList<String> alIdFields = new ArrayList<String>(
			Arrays.asList(idFields));
	String dbDriverOracle = "oracle.jdbc.driver.OracleDriver";
	String dbUrlOracle = "jdbc:oracle:thin:@//192.168.30.21:1521/"
			+ databaseOracle;
	String testOwner = dbUsernameOracle;
	String sourceOwner = dbUsernameOracle;
	Statement stmt;
	ResultSet rset;
	int total;
	LinkedHashMap<String, String> valueLables = new LinkedHashMap<String, String>();
	LinkedHashMap<String, Integer> matrixColumnsIndexSource = new LinkedHashMap<String, Integer>();
	LinkedHashMap<String, Integer> matrixColumnsIndexFound = new LinkedHashMap<String, Integer>();
	LinkedHashMap<String, Integer> matrixColumnsIndexNotFound = new LinkedHashMap<String, Integer>();
	LinkedHashMap<String, ArrayList<String>> tablesColumnsFound = new LinkedHashMap<String, ArrayList<String>>();
	LinkedHashMap<String, ArrayList<String>> tablesColumnsNotFound = new LinkedHashMap<String, ArrayList<String>>();

	@Test
	public void testInit() throws Exception {
		init();
		makeIndex();
		getTotalCount();
	}

	@Test(dependsOnMethods = { "testInit" })
	public void testCompare() throws Exception {
		if (compare()) {
			Assert.assertFalse(true);
		}
	}

	public void init() throws IOException {
		Locale.setDefault(Locale.US);
		System.out
				.println("To use outputed SQL from this script in any SQL client execute next line first (once per session) to set the date format right: ");
		System.out.println(sqlSetDateFormat);
		System.out.println();
		System.out.println("Oracle Database: " + databaseOracle);
		System.out.println("Oracle Username: " + dbUsernameOracle);
		System.out.print("Enter Oracle password: ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String dbPasswordOracle = null;
		dbPasswordOracle = br.readLine();
		try {
			Class.forName(dbDriverOracle);
			Connection conn = DriverManager.getConnection(dbUrlOracle,
					dbUsernameOracle, dbPasswordOracle);
			stmt = conn.createStatement();
			stmt.executeQuery(sqlSetDateFormat);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void makeIndex() throws Exception {
		// Get columns from CSV header
		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(file), "UTF-8"));
		String[] headerLine = reader.readNext();
		for (int i = 0; i < headerLine.length; i++)
			if (headerLine[i].length() != 0)
				matrixColumnsIndexSource.put(headerLine[i], i);

		// Make table, columns sets from CSV columns that match meta data.
		rset = stmt.executeQuery(dict);
		while (rset.next()) {
			String table = rset.getString(1);
			String column = rset.getString(2);
			if (matrixColumnsIndexSource.containsKey(column)) {
				addKeyValue(tablesColumnsFound, table, column);
				matrixColumnsIndexFound.put(column,
						matrixColumnsIndexSource.get(column));
			} else
				addKeyValue(tablesColumnsNotFound, table, column);
		}
		rset.close();

		// Remove tables with only ID columns.
		ArrayList<String> removeTables = new ArrayList<String>();
		for (Map.Entry<String, ArrayList<String>> tableColumns : tablesColumnsFound
				.entrySet()) {
			boolean remove = true;
			for (String column : tableColumns.getValue()) {
				if (alIdFields.contains(column) == false)
					remove = false;
			}
			if (remove)
				removeTables.add(tableColumns.getKey());
		}
		for (String removeTable : removeTables) {
			tablesColumnsFound.remove(removeTable);
		}

		// Get CSV columns not matching meta data.
		for (String matrixColumn : matrixColumnsIndexSource.keySet()) {
			if (matrixColumnsIndexFound.containsKey(matrixColumn) == false)
				matrixColumnsIndexNotFound.put(matrixColumn,
						matrixColumnsIndexSource.get(matrixColumn));
		}

		// Make the value labels conversion hash map.
		rset = stmt.executeQuery(dictValues);
		while (rset.next()) {
			valueLables.put(
					rset.getString(1) + rset.getString(2) + rset.getString(3),
					rset.getString(4));
		}
		rset.close();

		// Console output
		System.out
				.println("Metadata found in Matrix columns. Matrix {index=column}: "
						+ matrixColumnsIndexFound);
		System.out
				.println("Metadata found in Matrix columns. Metadata {table=[colums]}: "
						+ tablesColumnsFound);
		System.out
				.println("WARNING METADATA NOT FOUND IN MATRIX COLUMNS. Metadata {table=[colums]}: "
						+ tablesColumnsNotFound);
		System.out
				.println("WARNING MATRIX COLUMNS NOT FOUND IN METADATA. Matrix {index=column}: "
						+ matrixColumnsIndexNotFound);
	}

	public void getTotalCount() throws Exception {
		for (String table : tablesColumnsFound.keySet()) {
			rset = stmt.executeQuery("select count(*) from " + table);
			rset.next();
			total += rset.getInt(1);
			rset.close();
		}
		total = total * 4;
	}

	public boolean compare() throws Exception {
		boolean fail = false;
		int totalCounter = 0;
		int stepCounter = 0;

		for (Map.Entry<String, ArrayList<String>> tableColumns : tablesColumnsFound
				.entrySet()) {
			String table = tableColumns.getKey();
			ArrayList<String> columns = tableColumns.getValue();
			List<String[]> csvData = new ArrayList<String[]>();
			List<String[]> dbData = new ArrayList<String[]>();

			// Get DB data for table.
			System.out.println("Get DB data for table: " + table);
			String sql = "select ";
			for (String column : tableColumns.getValue())
				sql += "to_char(" + column + "), ";
			sql = sql.substring(0, sql.length() - 2) + " from " + table;
			rset = stmt.executeQuery(sql);
			while (rset.next()) {
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + total + "]");
				}
				stepCounter++;
				totalCounter++;
				List<String> lRow = new ArrayList<String>();
				boolean addLine = false;
				for (int i = 1; i <= columns.size(); i++) {
					if (rset.getString(i) == null
							|| rset.getString(i).toLowerCase().equals("null")
							|| rset.getString(i).equals(""))
						lRow.add("");
					else if (rset.getString(i).substring(0, 1).equals(".")
							&& ("0" + rset.getString(i))
									.matches("[-+]?\\d+(\\.\\d+)?")) {
						lRow.add(("0" + rset.getString(i)));
						if (alIdFields.contains(columns.get(i-1)) == false)
							addLine = true;
					} else {
						lRow.add(rset.getString(i));
						if (alIdFields.contains(columns.get(i-1)) == false)
							addLine = true;
					}
				}
				// Filter null values.
				if (addLine) {
					String[] saRow = lRow.toArray(new String[lRow.size()]);
					dbData.add(saRow);
				}
			}
			rset.close();

			// Get CSV data for table.
			System.out.println("Get CSV data for table: " + table);
			CSVReader reader = new CSVReader(new InputStreamReader(
					new FileInputStream(file), "UTF-8"));
			String[] line;
			reader.readNext();
			while ((line = reader.readNext()) != null) {
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + total + "]");
				}
				stepCounter++;
				totalCounter++;
				List<String> lRow = new ArrayList<String>();
				boolean addLine = false;
				for (String column : columns) {
					String value = line[matrixColumnsIndexFound.get(column)];
					if (valueLables.containsKey(table + column + value))
						value = valueLables.get(table + column + value);
					if (value == null || value.toLowerCase().equals("null")
							|| value.equals(""))
						lRow.add("");
					else {
						lRow.add(value);
						if (alIdFields.contains(column) == false)
							addLine = true;
					}
				}
				// Filter null values.
				if (addLine) {
					String[] saRow = lRow.toArray(new String[lRow.size()]);
					csvData.add(saRow);
				}
			}

			// Lookup DB in CSV data.
			int count;
			System.out.println("Lookup DB in CSV data: " + table);
			for (String[] lineparts : dbData) {
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + total + "]");
				}
				stepCounter++;
				totalCounter++;
				count = linePartsInLinePartsList(lineparts, csvData);
				if (count == 0) {
					fail = true;
					System.out.println("FAILED. NOT FOUND IN CSV: "
							+ sqlReconstruction(table, columns, lineparts));
				}
				if (count > 1) {
					if (count != linePartsInLinePartsList(lineparts, dbData)) {
						fail = true;
						System.out.println("FAILED. DIFFERENT ROWCOUNT: "
								+ sqlReconstruction(table, columns, lineparts));
					}
				}
			}

			// Lookup CSV in DB data.
			System.out.println("Lookup CSV in DB data: " + table);
			for (String[] lineparts : csvData) {
				if (stepCounter >= 500) {
					stepCounter = 0;
					System.out.println("[" + totalCounter + "/" + total + "]");
				}
				stepCounter++;
				totalCounter++;
				count = linePartsInLinePartsList(lineparts, dbData);
				if (count == 0) {
					fail = true;
					System.out.println("FAILED. NOT FOUND IN DB: "
							+ sqlReconstruction(table, columns, lineparts));
				}
				if (count > 1) {
					if (count != linePartsInLinePartsList(lineparts, csvData)) {
						fail = true;
						System.out.println("FAILED. DIFFERENT ROWCOUNT: "
								+ sqlReconstruction(table, columns, lineparts));
					}
				}
			}

		}
		System.out.println("[" + totalCounter + "/" + total + "]");
		return fail;
	}

	// Helper function.
	public int linePartsInLinePartsList(String[] lineParts,
			List<String[]> linePartsList) {
		int count = 0;
		for (String[] lineParts1 : linePartsList) {
			if (Arrays.equals(lineParts, lineParts1))
				count++;
		}
		return count;
	}

	// Helper function.
	public void addKeyValue(LinkedHashMap<String, ArrayList<String>> m,
			String k, String v) {
		if (m.get(k) == null) {
			ArrayList<String> dbvs = new ArrayList<String>();
			dbvs.add(v);
			m.put(k, dbvs);
		} else {
			ArrayList<String> dbvs = new ArrayList<String>();
			dbvs = m.get(k);
			if (dbvs.contains(v) == false)
				dbvs.add(v);
			m.put(k, dbvs);
		}
	}

	// Helper function.
	public String sqlReconstruction(String table, ArrayList<String> columns,
			String[] lineParts) {
		String sql = "select ";
		for (String column : columns)
			sql += column + ", ";
		sql = sql.substring(0, sql.length() - 2) + " from " + table + " where ";
		for (int i = 0; i < lineParts.length; i++) {
			// Handle null values
			if (lineParts[i] == null || lineParts[i].equals("")
					|| lineParts[i].toLowerCase().equals("null"))
				sql += columns.get(i) + " is null and ";
			// Handle numeric values
			else if (lineParts[i].matches("[-+]?\\d+(\\.\\d+)?"))
				sql += columns.get(i) + " = " + lineParts[i] + " and ";
			// Handle string values
			else
				sql += columns.get(i) + " = '"
						+ lineParts[i].replace("'", "''") + "' and ";
		}
		return sql.substring(0, sql.length() - 4);
	}

}
