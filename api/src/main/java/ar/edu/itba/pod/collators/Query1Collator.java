package ar.edu.itba.pod.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1Collator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).sorted((o1, o2) -> {
            int numberComparison = o2.getValue().compareTo(o1.getValue());
            return numberComparison == 0 ? o1.getKey().compareTo(o2.getKey()) : numberComparison;
        }).collect(Collectors.toList());
    }
}
