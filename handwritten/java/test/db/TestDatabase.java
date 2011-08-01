package test.db;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import junit.framework.TestCase;
import molgenis_test.fields.BoolEntity;
import molgenis_test.fields.DateEntity;
import molgenis_test.fields.DecimalEntity;
import molgenis_test.fields.EnumEntity;
import molgenis_test.fields.IntEntity;
import molgenis_test.fields.MrefEntity;
import molgenis_test.fields.TextEntity;
import molgenis_test.fields.VarcharEntity;
import molgenis_test.fields.XrefEntity;

import org.apache.log4j.Logger;
import org.molgenis.framework.db.Database;
import org.molgenis.framework.db.DatabaseException;
import org.molgenis.util.Entity;

import app.servlet.MolgenisServlet;


public class TestDatabase extends TestCase
{
	Database db = null;
	Logger logger = Logger.getLogger(TestDatabase.class);

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

	public void testVarchar() throws Exception
	{
		helper(VarcharEntity.class, "varchar", "test1", "test2", "1");
	}

	public void testText() throws Exception
	{
		helper(TextEntity.class, "text", "test1", "test2", "1");
	}

	public void testInt() throws Exception
	{
		helper(IntEntity.class, "int", 1, 2, 1);
	}

	public void testDecimal() throws Exception
	{
		helper(DecimalEntity.class, "decimal", 1.0, 2.0, 2.0);
	}

	public void testBool() throws Exception
	{
		helper(BoolEntity.class, "bool", true, true, true);
	}

	public void testEnum() throws Exception
	{
		helper(EnumEntity.class, "enum", "a", "b", "b");
	}

//	public void testDate() throws DatabaseException, IOException, ParseException, InstantiationException,
//			IllegalAccessException
//	{
//		DateFormat formatter = new SimpleDateFormat("MMMM d, yyyy", Locale.US);
//		formatter.setTimeZone(TimeZone.getDefault());
//
////		helper(DateEntity.class, "date", formatter.parse("Jan 1, 2001"), formatter.parse("Jan 2, 2001"), formatter
////				.parse("Feb 25, 2006"));
//		
//		helper(DateEntity.class, "date", "Jan 1, 2001", "Jan 2, 2001", DateEntity.string2date("Feb 25, 2006"));
//	}

	public void testDateTime() throws Exception
	{
		DateFormat formatter = new SimpleDateFormat("MMMM d, yyyy, hh:mm:ss", Locale.US);
		formatter.setTimeZone(TimeZone.getDefault());

		helper(DateEntity.class, "date", formatter.parse("Jan 1, 2001, 00:00:00"), formatter
				.parse("Jan 2, 2001, 00:00:00"), formatter.parse("Feb 25, 2006, 00:00:00"));
	}

	public void testXref() throws DatabaseException, IOException
	{
		try
		{
			db.remove(db.find(VarcharEntity.class));

			// create varcharentity
			VarcharEntity v = new VarcharEntity();
			v.setNormalVarchar("test1");
			v.setReadonlyVarchar("test1");

			// get id
			db.add(v);

			// create valid xref
			XrefEntity x1 = new XrefEntity();
			x1.setNormalXref(v);
			//shorthand for x1.setNormalXref(v.getId());
			x1.setReadonlyXref(v.getId());

			db.add(x1);

			// try change readonly xref
			x1.setReadonlyXref(2);
			db.update(x1);

			x1 = db.find(XrefEntity.class).get(0);
			assertEquals(x1.getReadonlyXref(), v.getId());

			// check label
			assertEquals(x1.getNormalXref_NormalVarchar(), v.getNormalVarchar());

			// create invalid xref
			try
			{
				x1.setNormalXref(-1);
				db.update(x1);
				fail("updated invalid xref");

			}
			catch (Exception e)
			{
			}

			// cleanup
			db.remove(x1);
			db.remove(v);

		}
		finally
		{
			db.remove(db.find(XrefEntity.class));
			db.remove(db.find(VarcharEntity.class));
		}
	}
	
	public void testMref() throws DatabaseException, IOException
	{
		try
		{
			db.remove(db.find(VarcharEntity.class));

			// create varcharentity
			VarcharEntity v = new VarcharEntity();
			v.setNormalVarchar("test1");
			v.setReadonlyVarchar("test1");

			// get id
			db.add(v);

			// create valid xref
			MrefEntity x1 = new MrefEntity();
			x1.setName("test");
			x1.getNormalMref_Id().add(v.getId());
			//shorthand for x1.setNormalXref(v.getId());
			x1.getNormalMref_Id().add(v.getId());

			db.add(x1);

			// try change readonly xref
			//x1.setReadonlyXref(2);
			//db.update(x1);

			x1 = db.find(MrefEntity.class).get(0);
			assertEquals(x1.getNormalMref().get(0), v.getId());

			// check label
			assertEquals(x1.getNillableMref_NormalVarchar().get(0), v.getNormalVarchar());

			// create invalid xref
			try
			{
				//x1.getNormalMref().re;
				db.update(x1);
				fail("updated invalid xref");

			}
			catch (Exception e)
			{
			}

			// cleanup
			db.remove(x1);
			db.remove(v);

		}
		finally
		{
			db.remove(db.find(MrefEntity.class));
			db.remove(db.find(VarcharEntity.class));
		}
	}

	// uses find, count, add, update, remove
	private <E extends Entity> void helper(Class<E> entityClass, String type, Object value1, Object value2,
			Object defaultValue) throws Exception
	{

		// cleanup
		db.remove(db.find(entityClass));

		// test nillable
		// test default
		Entity e = entityClass.newInstance();
		e.set("normal" + type, value1);
		e.set("nillable" + type, null);
		e.set("readonly" + type, value1);
		// don't set default

		db.add(e);

		// now id and default should be set
		this.assertNotNull(e.get("id"));
		this.assertEquals(defaultValue, e.get("default" + type));

		// try to update readonly: should not be persisted)
		e.set("normal" + type, value2);
		e.set("nillable" + type, value2);
		e.set("readonly" + type, value2);
		e.set("default" + type, value2);

		db.update(e);

		// find it back
		e = db.find(entityClass).get(0);
		assertEquals(e.get("normal" + type), value2);
		assertEquals(e.get("default" + type), value2);
		assertEquals(e.get("nillable" + type), value2);
		// readonly should not be changed
		assertEquals(e.get("readonly" + type), value1);

		// remove, afterwards count should be 0
		db.remove(e);
		assertEquals(0, db.count(e.getClass()));
	}
	
//	public void testFile() throws DatabaseException, IOException
//	{
//		File attachment = new File("testdata/testin.txt");
//		assertTrue(attachment.exists());
//		
//		FileEntity f = new FileEntity();
//		//get rid of this
//		f.setNormalFileAttachedFile(attachment);
//		f.setReadonlyFileAttachedFile(attachment);
//		
//		db.add(f);
//		
//		//find back
//		f = db.find(FileEntity.class).get(0);
//		
//		//compare files
//		File copy = new File(f.getNormalFile());
//		assertNotNull(copy);
//		assertTrue(copy.exists());
//		//fail("todo");		
//		
//		db.remove(db.find(FileEntity.class));
//	}
}
