package org.kiji.scoringbenchmark.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Random;

import com.google.common.io.LineReader;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.filter.StripValueColumnFilter;

/**
 * Collects a list of entities from a specified table.
 */
public class EntityIdCollector {
  /**
   * Collects a specified number of entity ids from a given table.
   * @param maxToCollect maximum number of entities to collect.
   */
  public static void collect(KijiTable table, File outFile, int maxToCollect) throws IOException {
    final KijiDataRequestBuilder requestBuilder = KijiDataRequest.builder();
    requestBuilder
        .newColumnsDef()
        .withFilter(new StripValueColumnFilter())
        .withMaxVersions(1)
        .add("data", "game_mode");
    final KijiDataRequest request = requestBuilder.build();
    final KijiRowScanner scanner = table.openTableReader().getScanner(request);
    final Iterator<KijiRowData> iterator = scanner.iterator();
    final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    for (int i = 0; i < maxToCollect; i++) {
      if (iterator.hasNext()) {
        if (i % 1000 == 0) {
          System.out.println(String.format("%s entities collected.", i));
        }
        final KijiRowData data = iterator.next();
        writer.println(Bytes.toString(data.getEntityId().getHBaseRowKey()));
      } else {
        break;
      }
    }
    writer.flush();
    writer.close();
  }

  public static void reservoir(File inFile, File outFile, int reservoirSize) throws IOException {
    final String[] reservoir = new String[reservoirSize];
    final LineReader reader = new LineReader(new FileReader(inFile));
    final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    int linesRead = 0;
    String line = reader.readLine();
    while (line != null) {
      if (linesRead < reservoirSize) {
        reservoir[linesRead] = line;
      } else {
        final Random rand = new Random();
        final int next = rand.nextInt(linesRead);
        if (next < reservoirSize) {
          reservoir[next] = line;
        }
      }
      line = reader.readLine();
      linesRead++;
    }
    for (String s : reservoir) {
      writer.println(s);
    }
    writer.flush();
    writer.close();
  }

  private static void appendToFile(File outFile, String lineToWrite) throws IOException {
    final PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    pw.println(lineToWrite);
    pw.flush();
    pw.close();
  }
}
