package trident.kestrel;

import java.util.List;

import com.google.common.collect.Lists;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class KestrelEnqueue extends BaseStateUpdater<KestrelState> {
  public void updateState(KestrelState kestrelState, List<TridentTuple> tuples,
      TridentCollector collector) {
    List<Object> itemsToEnqueue = Lists.newArrayList();
    for(TridentTuple t: tuples) {
      itemsToEnqueue.add(t.getValue(0));
    }
    kestrelState.enqueue(itemsToEnqueue);
  }
}