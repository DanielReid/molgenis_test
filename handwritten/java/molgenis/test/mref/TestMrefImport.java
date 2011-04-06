package molgenis.test.mref;

import java.io.File;

import org.molgenis.framework.db.Database;

import app.CsvImport;
import app.JDBCDatabase;
import app.servlet.MolgenisServlet;

public class TestMrefImport
{
	
	
	public static void main(String[] args) throws Exception
	{
		File directory = new File(TestMrefImport.class.getResource("").getFile());
		System.out.println("Importing from dir "+directory);
		JDBCDatabase db = null;
		try
		{
			MolgenisServlet m = new MolgenisServlet();
			db = (JDBCDatabase) m.getDatabase();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CsvImport.importAll(directory, db, null);

	

	}
}
