import org.molgenis.Molgenis;

public class MolgenisReplaceDatabase
{
	public static void main(String[] args) throws Exception
	{
		new Molgenis("molgenis.properties").updateDb();
	}
}
