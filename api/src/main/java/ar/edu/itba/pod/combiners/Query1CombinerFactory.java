package ar.edu.itba.pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1CombinerFactory implements CombinerFactory<String,Long,Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String stationName) {
        return new QueryCombiner();
    }

    private static class QueryCombiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public  void combine(Long isMember) {
            sum += isMember;
        }

        @Override
        public void reset() {
            sum = 0;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }
    }
}
