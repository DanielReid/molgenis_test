package test.web;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Joiner;

public class DataTestMatrixEAV {
	String file;
	String investigation;
	boolean filter1 = false;
	boolean filter2 = false;
	String filter1Table = "";
	String filter1Column = "";
	String filter1Operator = "";
	String filter1Value = "";
	String filter2Table = "";
	String filter2Column = "";
	String filter2Operator = "";
	String filter2Value = "";
	String whereCondition = "";
	String testTablePrefix = "T2_";
	String sourceTablePrefix = "LL_";
	String matrixSeperator = "\t";
	String testOwner = "MOLGENIS";
	Map<Integer, String> matrixColumnsIndex = new LinkedHashMap<Integer, String>();
	Map<Integer, String> matrixDbTablesIndex = new LinkedHashMap<Integer, String>();
	Map<Integer, String> matrixDbColumnsIndex = new LinkedHashMap<Integer, String>();

	public static Connection getConnection() throws Exception {
		Locale.setDefault(Locale.US);
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@//localhost:2000/llptest";
		String username = "molgenis";
		String password = "molTagtGen24Ora";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, username, password);
		return conn;
	}

	public ResultSet executeQuery(String sql) throws Exception {
		Connection conn = DataTestMatrix.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rset = stmt.executeQuery(sql);
		return rset;

	}

	public boolean tableExistsEAV(String table) throws Exception {
		ResultSet rset = executeQuery("select count(*) from protocol where name = '"
				+ sourceTablePrefix
				+ table
				+ "' and investigation = '"
				+ investigation + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	public boolean columnExistsEAV(String column) throws Exception {
		ResultSet rset = executeQuery("select count(*) from observationelement where name = '"
				+ column + "' and investigation = '" + investigation + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	public boolean tableExists(String owner, String table) throws Exception {
		ResultSet rset = executeQuery("select count(*) from dba_all_tables where owner='"
				+ owner + "' and table_name='" + table + "'");
		rset.next();
		if (rset.getString(1).equals("1"))
			return true;
		return false;
	}

	public String getGlobalTablePaidColumnName() {
		for (Map.Entry<Integer, String> matrixDbColumn : matrixDbColumnsIndex
				.entrySet()) {
			if (matrixDbColumn.getValue().equals("PA_ID"))
				return "c" + matrixDbColumn.getKey();
		}
		return "";
	}

	public boolean parseMatrixColumns() throws Exception {
		System.out.println("Start parsing and lookup Matrix colums...");
		boolean result = true;
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine = br.readLine();
		String[] matrixColumns = strLine.split(matrixSeperator);
		for (int i = 0; i < matrixColumns.length; i++) {
			String[] matrixColumnParts = matrixColumns[i].split("__");
			if (matrixColumnParts.length == 2) {
				if (tableExistsEAV(matrixColumnParts[0])
						&& columnExistsEAV(matrixColumnParts[1])) {
					matrixColumnsIndex.put(i, matrixColumns[i]);
					matrixDbTablesIndex.put(i, matrixColumnParts[0]);
					matrixDbColumnsIndex.put(i, matrixColumnParts[1]);
				} else {
					System.out.println("FAIL: '" + matrixColumnParts[0] + "."
							+ matrixColumnParts[1]
							+ "' (based on matrix column) not in source");
					result = false;
				}
			} else {
				if (matrixColumnParts[0].equals("")
						|| matrixColumnParts[0].equals("ROWNUMBER")) {
				} else {
					System.out
							.println("FAIL: '"
									+ matrixColumnParts[0]
									+ "' not conform format. (can not extract the source table and column name)");
					result = false;
				}
			}
		}
		in.close();
		System.out.println("Parsing and lookup Matrix colums done...");
		return result;
	}

	public void makeGlobalTable() throws Exception {
		if (tableExists(testOwner, testTablePrefix + "GLOBAL"))
			executeQuery("drop table " + testTablePrefix + "GLOBAL");
		String sql = "create table "
				+ testTablePrefix
				+ "GLOBAL (c"
				+ Joiner.on(" varchar(255), c").join(
						matrixColumnsIndex.keySet()) + "  varchar(255))";
		executeQuery(sql);
	}

	public void fillGlobalTable() throws Exception {
		System.out.println("Start insertation...");
		String sqlHead = "";
		String sqlBody = "";
		Integer counter = 0;
		Integer counterTotal = 0;
		String line;
		String[] lineParts;
		FileInputStream fstream = new FileInputStream(file);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		// Skip first line: column names.
		br.readLine();
		// First data line: SQL output contains: "select ".
		sqlHead += "insert into " + testTablePrefix + "GLOBAL (c"
				+ Joiner.on(", c").join(matrixColumnsIndex.keySet()) + ")\n";
		if ((line = br.readLine()) != null) {
			lineParts = line.split(matrixSeperator);
			sqlHead += "select ";
			for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
					.entrySet()) {
				sqlHead += "'" + lineParts[matrixColumn.getKey()] + "', ";
			}
			sqlHead = sqlHead.substring(0, sqlHead.length() - 2); // Remove the
																	// last ", "
			sqlHead += " from dual\n";
		}
		// Flowing data lines: SQL output contains: "union all select ".
		while ((line = br.readLine()) != null) {
			lineParts = line.split(matrixSeperator);
			sqlBody += "union all select ";
			for (Map.Entry<Integer, String> matrixColumn : matrixColumnsIndex
					.entrySet()) {
				sqlBody += "'" + lineParts[matrixColumn.getKey()] + "', ";
			}
			// Remove the last ", "
			sqlBody = sqlBody.substring(0, sqlBody.length() - 2);
			sqlBody += " from dual\n";
			counter++;
			counterTotal++;
			// Prevent performance decrease (and provide feedback).
			if (counter >= 1000) {
				executeQuery(sqlHead + sqlBody);
				System.out.println(counterTotal + " rows inserted...");
				sqlBody = "";
				counter = 0;
			}
		}
		if (sqlBody.length() != 0)
		{
			executeQuery(sqlHead + sqlBody);
			System.out.println(counterTotal + " rows inserted...");
		}		
		System.out.println("Insertation done...");
		in.close();
	}

	public boolean compareGlobalTableToEAV() throws Exception {
		System.out.println("Starting compare please wait...");
		String globalTablePaidColumnName = getGlobalTablePaidColumnName();
		String sql = "";
		// Query global table.
		for (Map.Entry<Integer, String> matrixDbTable : matrixDbTablesIndex
				.entrySet()) {
			if (sql.length() != 0)
				sql += "\nunion \n\n";
			sql += "select \n";
			sql += "  " + globalTablePaidColumnName + " pa_id \n";
			sql += "  ,'" + sourceTablePrefix + matrixDbTable.getValue()
					+ "' tab \n";
			sql += "  ,'" + matrixDbColumnsIndex.get(matrixDbTable.getKey())
					+ "' col \n";
			sql += "  ,c" + matrixDbTable.getKey() + " value\n";
			sql += "from \n";
			sql += "  " + testTablePrefix + "global \n";
		}

		// Minus.
		sql += "\nminus \n\n";

		// Query EAV.
		sql += "select \n";
		sql += "  ( \n";
		sql += "  select \n";
		sql += "    value pa_id \n";
		sql += "  from \n";
		sql += "    observedvalue ov_1 \n";
		sql += "  join observationelement oe_1 \n";
		sql += "    on ov_1.feature = oe_1.id \n";
		sql += "  where \n";
		sql += "    ov_1.investigation = ov.investigation \n";
		sql += "    and ov_1.protocolapplication = ov.protocolapplication \n";
		sql += "    and oe_1.name = 'PA_ID' \n";
		sql += "  ) pa_id \n";
		sql += "  ,p.name tab \n";
		sql += "  ,oe.name col \n";
		sql += "  ,ov.value \n";
		sql += "from \n";
		sql += "  observedvalue ov \n";
		sql += "join observationelement oe \n";
		sql += "  on ov.feature = oe.id \n";
		sql += "  and ov.investigation = '" + investigation + "' \n";
		sql += "join protocolapplication pa \n";
		sql += "  on ov.protocolapplication = pa.id \n";
		sql += "join protocol p \n";
		sql += "  on pa.protocol = p.id \n";

		if (filter1 == true) {
			sql += "where (p.name = '" + sourceTablePrefix + filter1Table
					+ "' and oe.name = '" + filter1Column + "' and ov.value "
					+ filter1Operator + " '" + filter1Value + "')";
		}

		if (filter1 == true && filter2 == true) {
			sql += "or (p.name = '" + sourceTablePrefix + filter2Table
					+ "' and oe.name = '" + filter2Column + "' and ov.value "
					+ filter2Operator + " '" + filter2Value + "')";
		}
		System.out.println(sql);

		// Make a count.
		String sqlCount = "select count(*) from (" + sql + ")";
		ResultSet rset = executeQuery(sqlCount);
		rset.next();
		if (rset.getString(1).equals("0"))
			return true;
		System.out.println("FAIL: Row count NOT maching values = "
				+ rset.getString(1));
		System.out.println("The used query is: ");
		System.out.println(sql);
		return false;
	}

}
