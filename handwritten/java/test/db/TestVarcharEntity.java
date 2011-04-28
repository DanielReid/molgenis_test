package test.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;
import molgenis_test.fields.VarcharEntity;

import org.molgenis.framework.db.Database;
import org.molgenis.framework.db.DatabaseException;

import app.servlet.MolgenisServlet;

public class TestVarcharEntity  extends TestCase
{
	Database db = null;
	
	public void setUp()
	{
		try
		{
			MolgenisServlet m = new MolgenisServlet();
			db = m.getDatabase();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void testFindSyntax() throws FileNotFoundException, IOException, DatabaseException
	{
		VarcharEntity e = new VarcharEntity();
		e.setNormalVarchar("test3"+System.currentTimeMillis());
		e.setReadonlyVarchar("testr3");
		
		db.add(e);
		db.find(VarcharEntity.class);
		
		e = new VarcharEntity();
		e.setNormalVarchar("test4"+System.currentTimeMillis());
		e.setReadonlyVarchar("testr4");
		db.add(e);
		
		//db.sql("checkpoint"); //writes it out; should be automated
		List<VarcharEntity> result = db.find(VarcharEntity.class);
		this.assertEquals(2, result.size());
	}
	
	public void tearDown()
	{
		try {
			db.remove(db.find(VarcharEntity.class));
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
