package org.eclipse.epsilon.test.util;

import java.net.URISyntaxException;
import java.nio.file.Paths;

public class EpsilonTestUtil {
	private EpsilonTestUtil() {}
	
	/*
	 * Convenience hack for handling exceptions when resolving this class's package source directory.
	 */
	public static String getTestBaseDir(Class<?> clazz) {
		try {
			return Paths.get(clazz.getResource("").toURI()).toString().replace("bin", "src")+'/';
		}
		catch (URISyntaxException urx) {
			System.err.println(urx.getMessage());
			return null;
		}
	}
}
