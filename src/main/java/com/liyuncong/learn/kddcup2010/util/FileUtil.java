package com.liyuncong.learn.kddcup2010.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.liyuncong.learn.kddcup2010.exploratorydataanalysis.DataFiles;

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
	
	public static BufferedReader getBufferedReader(String pathName) throws FileNotFoundException, UnsupportedEncodingException {
		InputStream inputStream = new FileInputStream(pathName);
		Reader reader = new InputStreamReader(inputStream, "utf-8");
		return new BufferedReader(reader, 4096);
	}
	
	public static FileLineIterator getFileLineIterator(BufferedReader bufferedReader) {
		return new FileLineIterator(bufferedReader);
	}
	
	public static class FileLineIterator implements Iterator<String> {
		private BufferedReader bufferedReader;
		private String nextLine;

		public FileLineIterator(BufferedReader bufferedReader) {
			this.bufferedReader = bufferedReader;
		}
		
		@Override
		public boolean hasNext() {
			try {
				return (nextLine = bufferedReader.readLine()) != null;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return false;
		}

		@Override
		public String next() {
			return nextLine;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
}
