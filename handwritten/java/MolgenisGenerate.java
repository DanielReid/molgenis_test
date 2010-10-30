import org.molgenis.Molgenis;


/**
 * This function will generate a molgenis, assuming a molgenis.properties file.
 */
public class MolgenisGenerate
{
	public static void main(String[] args) throws Exception
	{
		new Molgenis("molgenis.properties").generate();
	}
}
