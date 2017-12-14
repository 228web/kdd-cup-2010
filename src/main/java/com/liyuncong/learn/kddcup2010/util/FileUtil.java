package com.liyuncong.learn.kddcup2010.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author liyuncong
 *
 */
public class FileUtil {
	private FileUtil() {}
	
	public static List<String> head(int n, String filePath) throws FileNotFoundException, IOException {
		List<String> result = new LinkedList<>();
		try(InputStream inputStream = new FileInputStream(filePath);
				Reader reader = new InputStreamReader(inputStream, "utf-8");
				BufferedReader bufferedReader = new BufferedReader(reader, 4096)) {
			String line;
			int count = 0;
			while ((line = bufferedReader.readLine()) != null && count++ < n) {
				result.add(line);
			}
		}
		return result;
	}
}
