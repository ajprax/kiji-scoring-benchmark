package org.kiji.scoringbenchmark.gather;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.filter.StripValueColumnFilter;

public class EntityGatherer extends KijiGatherer {
  @Override
  public KijiDataRequest getDataRequest() {
    final KijiDataRequestBuilder requestBuilder = KijiDataRequest.builder();
    requestBuilder
        .newColumnsDef()
        .withFilter(new StripValueColumnFilter())
        .withMaxVersions(1)
        .add("data", "game_mode");
    return requestBuilder.build();
  }

  @Override
  public void gather(final KijiRowData input, final GathererContext context) throws IOException {
    context.write(new LongWritable((Long) input.getEntityId().getComponents().get(0)), NullWritable.get());
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
