package org.kiji.scoringbenchmark.produce;

import java.io.IOException;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

public class ResetTableProducer extends KijiProducer {
  KijiTableWriter writer = null;


  @Override
  public void setup(KijiContext context) throws IOException {
    final KijiURI tableURI = KijiURI.newBuilder("kiji://(xwing07.ul.wibidata.net,xwing09.ul.wibidata.net,xwing11.ul.wibidata.net):2181/wibidota/dota_matches").build();
    final Kiji kiji = Kiji.Factory.open(tableURI);
    try {
      final KijiTable table = kiji.openTable("dota_matches");
      try {
        writer = table.openTableWriter();
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  @Override
  public void cleanup(KijiContext context) throws IOException {
    writer.close();
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.create("derived_data", "fresh");
  }

  @Override
  public String getOutputColumn() {
    return "derived_data:fresh";
  }

  @Override
  public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
    if (input.containsColumn("derived_data", "fresh")) {
      writer.deleteColumn(input.getEntityId(), "derived_data", "fresh");
    }
  }
}
