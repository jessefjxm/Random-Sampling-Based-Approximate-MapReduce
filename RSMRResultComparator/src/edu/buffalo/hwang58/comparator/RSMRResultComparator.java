package edu.buffalo.hwang58.comparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RSMRResultComparator {
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: RSMRResultComparator <commonpath> <ratio1> <ratio2> ...[ratio3]");
			System.err.println("e.g.: ~/output/rate 1.0 0.5 0.25 0.1");
			System.err.println("Note: ratio should be in 0.0 to 1.0");
			System.err.println("Note: first ratio must be largest and is better to be 1.0");
			System.err.println("Note: output of mapreduce should named as [part-r-00000]");
			System.err.println("Note: output of rand mapreduce program should named as [result.txt]");
			System.exit(2);
		}
		List<List<String>> res = new ArrayList<List<String>>(args.length - 1);
		List<Double> ratio = new ArrayList<>(args.length - 1);
		List<FileReader> frs = new ArrayList<>(args.length - 1);
		List<BufferedReader> brs = new ArrayList<>(args.length - 1);
		List<String> lines = new ArrayList<>(args.length - 1);
		List<String[]> strs = new ArrayList<>(args.length - 1);
		List<Long> counts = new ArrayList<>(args.length - 1);
		List<Double> errorrates = new ArrayList<>(args.length - 1);

		long sum = 0;
		try {
			for (int i = 0; i < args.length - 1; i++) {
				res.add(new ArrayList<>());
				ratio.add(readResult(args[0] + args[i + 1], res.get(i)));
				frs.add(new FileReader(new File(args[0] + args[i + 1] + "/part-r-00000")));
				brs.add(new BufferedReader(frs.get(i)));
				lines.add(brs.get(i).readLine());

				strs.add(null);
				counts.add((long) 0);
				errorrates.add((double) 0);
			}
			do {
				for (int i = 0; i < args.length - 1; i++) {
					String line = lines.get(i);
					if (line != null) {
						strs.set(i, line.split("\t"));
						if (i != 0 || strs.get(i)[1].equals("")) {
							counts.set(i, (long) 0);
						} else {
							counts.set(i, Long.parseLong(strs.get(i)[1]));
						}
					} else {
						strs.set(i, null);
						counts.set(i, (long) 0);
					}
				}
				for (int i = 1; i < args.length - 1; i++) {
					if (strs.get(i) == null)
						continue;
					if (strs.get(0)[0].equals(strs.get(i)[0])) {
						counts.set(i, strs.get(i)[1].equals("") ? 0 : Long.parseLong(strs.get(i)[1]));
						lines.set(i, brs.get(i).readLine());
					}
				}
				sum += counts.get(0) / ratio.get(0);
				for (int i = 1; i < args.length - 1; i++) {
					double currate = errorrates.get(i);
					errorrates.set(i, currate
							+ Math.abs(1.0 * counts.get(0) - 1.0 * counts.get(i) * ratio.get(0) / ratio.get(i)));
				}
				lines.set(0, brs.get(0).readLine());
			} while (lines.get(0) != null);
			for (int i = 0; i < args.length - 1; i++) {
				frs.get(i).close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 1; i < args.length - 1; i++) {
			res.get(i).add("[Data error rate] " + String.format("%.2f", errorrates.get(i) / sum * 100) + "%");
		}

		for (int i = 0; i < args.length - 1; i++) {
			System.out.println("=== Result with sampling ratio " + ratio.get(i) + "===");
			for (String s : res.get(i))
				System.out.println(s);
		}
	}

	public static double readResult(String filepath, List<String> res) {
		double ratio = 0;
		try {
			File file = new File(filepath + "/result.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				res.add(line);
				if (line.contains("[Designed sample rate]")) {
					ratio = Double.parseDouble(line.substring(22, line.length() - 1)) / 100;
				}
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ratio;
	}
}
