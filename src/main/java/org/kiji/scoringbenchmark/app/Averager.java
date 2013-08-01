package org.kiji.scoringbenchmark.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.LineReader;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 8/1/13 Time: 10:08 AM To change this template use
 * File | Settings | File Templates.
 */
public class Averager {
  public static void calculateAverages(File inFile, File outFile) throws IOException {
    final List<Double> times = Lists.newArrayList();
    final LineReader reader = new LineReader(new FileReader(inFile));
    String line = reader.readLine();
    while (line != null) {
      final String[] tokens = line.split(" ");
      times.add(Double.valueOf(tokens[tokens.length - 1]));
      line = reader.readLine();
    }
    calculateMean(outFile, times);
    calculateMedian(outFile, times);
    bucket(outFile, times);
  }

  private static void calculateMean(File outFile, List<Double> doubles) throws IOException {
    Double total = 0.0;
    int index = 0;
    for (Double d : doubles) {
      total += d;
      if (d > 100000000) {
        appendToFile(outFile,
            String.format("record #%d took %s nanoseconds", index, d.longValue()));
      }
      index++;
    }
    appendToFile(outFile,
        String.format("Mean time was: %s", Double.valueOf((total / doubles.size())).longValue()));
    }

  private static void calculateMedian(File outFile, List<Double> doubles) throws IOException {
    final List<Double> copy = Lists.newArrayList(doubles);
    Collections.sort(copy);
    appendToFile(outFile,
        String.format("Median time was: %s", copy.toArray(new Double[copy.size()])[copy.size() / 2]));
  }

  private static void bucket(File outFile, List<Double> doubles) throws IOException {
    long[] splits = new long[7];
    splits[0] = 1000000l;
    splits[1] = 10000000l;
    splits[2] = 100000000l;
    splits[3] = 1000000000l;
    splits[4] = 10000000000l;
    splits[5] = 100000000000l;
    splits[6] = 1000000000000l;
    int[] counts = new int[7];
    for (Double d : doubles) {
      if (d < splits[0]) {
        counts[0]++;
      } else if (d < splits[1]) {
        counts[1]++;
      } else if (d < splits[2]) {
        counts[2]++;
      } else if (d < splits[3]) {
        counts[3]++;
      } else if (d < splits[4]) {
        counts[4]++;
      } else if (d < splits[5]) {
        counts[5]++;
      } else if (d < splits[6]) {
        counts[6]++;
      }
    }
    appendToFile(outFile,
        String.format("%d requests took less than 1000000 nanoseconds", counts[0]));
    appendToFile(outFile,
        String.format("%d requests took less than 10000000 nanoseconds", counts[1]));
    appendToFile(outFile,
        String.format("%d requests took less than 100000000 nanoseconds", counts[2]));
    appendToFile(outFile,
        String.format("%d requests took less than 1000000000 nanoseconds", counts[3]));
    appendToFile(outFile,
        String.format("%d requests took less than 10000000000 nanoseconds", counts[4]));
    appendToFile(outFile,
        String.format("%d requests took less than 100000000000 nanoseconds", counts[5]));
    appendToFile(outFile,
        String.format("%d requests took less than 1000000000000 nanoseconds", counts[6]));
  }

  private static void appendToFile(File outFile, String lineToWrite) throws IOException {
    final PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    pw.println(lineToWrite);
    pw.flush();
    pw.close();
  }
}
