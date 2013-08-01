package org.kiji.scoringbenchmark.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.google.common.io.LineReader;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReaderBuilder;

public class Requester {
  public static void request(KijiTable table, KijiDataRequest request, File inFile, File outFile)
      throws IOException {
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(table)
        .withTimeout(10000)
        .build();
    final LineReader reader = new LineReader(new FileReader(inFile));
    String line = reader.readLine();
    int linesRead = 1;
    final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    while (line != null) {
      if (linesRead % 1000 == 0) {
        System.out.println(String.format("%s entities scored.", linesRead));
      }
      final EntityId eid = HBaseEntityId.fromHBaseRowKey(Bytes.toBytes(line));
      final long startTime = System.nanoTime();
      final KijiRowData data = freshReader.get(eid, request);
      final long endTime = System.nanoTime();
      writer.println(String.format("Row: %s started at time: %d and took (nanos): %d",
          line, startTime, endTime - startTime));
      if (endTime - startTime > 10000000000l) {
        System.out.println(String.format(
            "request for row %s took %s nanoseconds", line, endTime - startTime));
      }
      line = reader.readLine();
      linesRead++;
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
