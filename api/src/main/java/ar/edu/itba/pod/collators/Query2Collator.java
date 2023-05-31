package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.models.Journey;
import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2Collator implements Collator<Map.Entry<String, Journey>, List<Map.Entry<String, Journey>>> {
    @Override
    public List<Map.Entry<String, Journey>> collate(Iterable<Map.Entry<String, Journey>> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).sorted((o1, o2) -> {
            int numberComparison = o2.getValue().compareTo(o1.getValue());
            return numberComparison == 0 ? o1.getKey().compareTo(o2.getKey()) : numberComparison;
        }).collect(Collectors.toList());
    }
}
