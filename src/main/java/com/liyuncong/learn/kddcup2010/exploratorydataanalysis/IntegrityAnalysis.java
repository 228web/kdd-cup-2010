package com.liyuncong.learn.kddcup2010.exploratorydataanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

public class IntegrityAnalysis {
	public static void main(String[] args) throws FileNotFoundException, IOException {
		try(InputStream inputStream = new FileInputStream(DataFiles.algebra_2008_2009_train);
				Reader reader = new InputStreamReader(inputStream, "utf-8");
				BufferedReader bufferedReader = new BufferedReader(reader, 4096)) {
			Set<Integer> exceptionLength = new HashSet<>();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				if (line.split("\t").length == 6) {
					System.out.println(line + "\tlength=");
					exceptionLength.add(line.split("\t").length);
				}
			}
			for (Integer integer : exceptionLength) {
				System.out.println(integer);
			}
		}
	}
}
