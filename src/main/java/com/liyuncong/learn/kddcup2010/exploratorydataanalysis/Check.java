package com.liyuncong.learn.kddcup2010.exploratorydataanalysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.liyuncong.learn.kddcup2010.util.FileUtil;
import com.liyuncong.learn.kddcup2010.util.LinuxCommand;

public class Check {
	private Check() {}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		List<String> headLines = FileUtil.head(5, DataFiles.algebra_2008_2009_train);
		print(LinuxCommand.cut(headLines, "\t", 1, 2, 6));
		headLines = FileUtil.head(1, DataFiles.algebra_2008_2009_test);
		print(headLines);
		headLines = FileUtil.head(1, DataFiles.bridge_to_algebra_2008_2009_train);
		print(headLines);
		headLines = FileUtil.head(1, DataFiles.bridge_to_algebra_2008_2009_test);
		print(headLines);
	}
	
	public static void print(List<String> lines) {
		System.out.println("---------------------");
		for (String line : lines) {
			System.out.println(line);
		}
	}
}
