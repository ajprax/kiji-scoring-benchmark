package org.kiji.scoringbenchmark.produce;

import java.io.IOException;

import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

public class PercentAnonymousProducer extends KijiProducer {

  private static final int ANONYMOUS_PLAYER_ID = -1;


  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.create("data", "player_data");
  }

  @Override
  public String getOutputColumn() {
    return "derived_data:fresh";
  }

  @Override
  public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
    final Players players = input.getMostRecentValue("data", "player_data");
    double totalPlayers = 0;
    double anonymousPlayers = 0;
    for (Player player : players.getPlayers()) {
      if (player.getLeaverStatus() != null) {
        totalPlayers++;
        if (player.getAccountId() == ANONYMOUS_PLAYER_ID) {
          anonymousPlayers++;
        }
      }
    }
    context.put(anonymousPlayers / totalPlayers);
  }
}
