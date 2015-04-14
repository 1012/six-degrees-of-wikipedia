package be.casisto.tinkering.wikiwiki.indexer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Helper class for the unit tests in this project.
 * 
 * @author jiri
 */
public class TestHelper {

	/**
	 * Reads the content of a file into a String.
	 * 
	 * @param file
	 *            path to the file
	 * @return
	 * @throws IOException
	 */
	public static String readFile(String file) throws IOException {
		byte[] bytes = Files.readAllBytes(Paths.get(file));
		return new String(bytes);
	}

}
