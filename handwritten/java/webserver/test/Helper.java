package webserver.test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

import org.molgenis.framework.db.Database;
import org.molgenis.util.TarGz;

import app.servlet.MolgenisServlet;

public class Helper {

	public static void deleteDatabase() throws Exception {
		File dbDir = new File("hsqldb");
		if (dbDir.exists()) {
			TarGz.recursiveDeleteContentIgnoreSvn(dbDir);
		} else {
			throw new Exception("HSQL database directory does not exist");
		}

		if (dbDir.list().length != 1) {
			throw new Exception(
					"HSQL database directory does not contain 1 file (.svn) after deletion! it contains: "
							+ dbDir.list().toString());
		}
	}

	/**
	 * Return the initial port number if it was available. Otherwise, increase
	 * with 1 over a given range until a free port was found. If none are found,
	 * throws IOException.
	 * 
	 * @param initialPort
	 * @param range
	 * @return
	 * @throws IOException
	 */
	public static int getAvailablePort(int initialPort, int range)
			throws IOException {
		for (int port = initialPort; port < (initialPort + range); port++) {
			boolean portTaken = false;
			ServerSocket socket = null;
			try {
				socket = new ServerSocket(port);
			} catch (IOException e) {
				portTaken = true;
			} finally {
				if (socket != null) {
					socket.close();
				}
			}
			if (!portTaken) {
				return port;
			}
		}
		throw new IOException(
				"All ports in the range "
						+ initialPort
						+ "-"
						+ (initialPort + range)
						+ " were unavailable. Select a different initial port or increase the scanning range.");
	}

	public static void main(String[] args) throws Exception {
		deleteDatabase();

	}

}
