package org.kiji.scoringbenchmark.app;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import com.google.common.base.Preconditions;

import org.kiji.common.flags.Flag;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.tools.BaseTool;

public class BenchmarkTool extends BaseTool {

  @Flag(name="do", required=true, usage=
      "\"collect-first (requires --out-file and --max-to-collect)\";"
      + "\"collect-random (requires --in-file, --out-file and --max-to-collect\""
      + "\"request (requires --columns, --in-file, and --out-file)\";"
      + "\"average (requires --in-file and --out-file\";")
  private String mDoFlag = "";

  @Flag(name="out-file", usage="path to the file into which to write tool output")
  private String mOutFileFlag = "";

  @Flag(name="max-to-collect", usage="maximum number of entities to collect.")
  private String mMaxFlag = "";

  @Flag(name="columns", usage="a comma separated list of kiji column names")
  private String mColumnsFlag = "";

  @Flag(name="in-file", usage="path to the file from which to read input")
  private String mInFileFlag = "";

  private enum DoMode {
    COLLECT_FIRST, COLLECT_RANDOM, REQUEST, AVERAGE,
  }

  private DoMode mDoMode;

  private int collectEIDs(KijiTable table, File outFile, int maxToCollect) throws IOException {
    EntityIdCollector.collect(table, outFile, maxToCollect);
    return BaseTool.SUCCESS;
  }

  private int randomizeEIDS(File inFile, File outFile, int reservoirSize) throws IOException {
    EntityIdCollector.reservoir(inFile, outFile, reservoirSize);
    return BaseTool.SUCCESS;
  }

  private int makeRequests(KijiTable table, KijiDataRequest request, File inFile, File outFile)
      throws IOException {
    Requester.request(table, request, inFile, outFile);
    return BaseTool.SUCCESS;
  }

  private int calculateAverages(File inFile, File outFile) throws IOException {
    Averager.calculateAverages(inFile, outFile);
    return BaseTool.SUCCESS;
  }

  public void validateFlags() throws Exception {
    try {
      mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT).replace("-", "_"));
    } catch (IllegalArgumentException iae) {
      getPrintStream().printf("Invalid --do command: '%s' .%n", mDoFlag);
      throw iae;
    }
  }

  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    switch (mDoMode) {
      case COLLECT_FIRST: {
        Preconditions.checkNotNull(nonFlagArgs,
            "Specify a Kiji table with \"kiji fresh kiji://hbase-address/kiji-instance/"
                + "kiji-table/\"");
        Preconditions.checkArgument(nonFlagArgs.size() >= 1,
            "Specify a Kiji table with \"kiji scoring-benchmark kiji://hbase-address/kiji-instance/"
                + "kiji-table/\"");
        final KijiURI uri;
        try {
          uri = KijiURI.newBuilder(nonFlagArgs.get(0)).build();
        } catch (KijiURIException kurie) {
          getPrintStream().format("Invalid KijiURI. Specify a Kiji table or column with \"kiji fresh"
              + " kiji://hbase-address/kiji-instance/kiji-table/[optional-kiji-column]\"");
          throw kurie;
        }
        final KijiTable table = Kiji.Factory.open(uri).openTable(uri.getTable());
        return collectEIDs(table, new File(mOutFileFlag), Integer.valueOf(mMaxFlag));
      }
      case COLLECT_RANDOM: {
        return randomizeEIDS(
            new File(mInFileFlag), new File(mOutFileFlag), Integer.valueOf(mMaxFlag));
      }
      case REQUEST: {
        Preconditions.checkNotNull(nonFlagArgs,
            "Specify a Kiji table with \"kiji fresh kiji://hbase-address/kiji-instance/"
                + "kiji-table/\"");
        Preconditions.checkArgument(nonFlagArgs.size() >= 1,
            "Specify a Kiji table with \"kiji scoring-benchmark kiji://hbase-address/kiji-instance/"
                + "kiji-table/\"");
        final KijiURI uri;
        try {
          uri = KijiURI.newBuilder(nonFlagArgs.get(0)).build();
        } catch (KijiURIException kurie) {
          getPrintStream().format("Invalid KijiURI. Specify a Kiji table or column with \"kiji fresh"
              + " kiji://hbase-address/kiji-instance/kiji-table/[optional-kiji-column]\"");
          throw kurie;
        }
        final KijiTable table = Kiji.Factory.open(uri).openTable(uri.getTable());
        final String[] columns = mColumnsFlag.split(",");
        final KijiDataRequestBuilder requestBuilder = KijiDataRequest.builder();
        for (String column : columns) {
          requestBuilder.newColumnsDef().add(new KijiColumnName(column));
        }
        final KijiDataRequest request = requestBuilder.build();
        return makeRequests(table, request, new File(mInFileFlag), new File(mOutFileFlag));
      }
      case AVERAGE: {
        final File inFile = new File(mInFileFlag);
        final File outFile = new File(mOutFileFlag);
        return calculateAverages(inFile, outFile);
      }
      default: {
        throw new InternalKijiError(String.format("unknown DoMode: %s.", mDoMode));
      }
    }
  }

  @Override
  public String getName() {
    return "scoring-benchmark";
  }

  @Override
  public String getDescription() {
    return "run benchmarking tests for kiji-scoring";
  }

  @Override
  public String getCategory() {
    return "benchmarking";
  }
}
