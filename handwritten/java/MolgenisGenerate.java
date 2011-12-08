import org.molgenis.Molgenis;
import org.molgenis.generators.sql.OracleCreateSubclassPerTableGen;


/**
 * This function will generate a molgenis, assuming a molgenis.properties file.
 */
public class MolgenisGenerate
{
	public static void main(String[] args) throws Exception
	{
		//new Molgenis("molgenis.properties", OracleCreateSubclassPerTableGen.class).generate();
		new Molgenis("molgenis.properties").generate();
	}
}
