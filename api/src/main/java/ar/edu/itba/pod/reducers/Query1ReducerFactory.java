package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1ReducerFactory implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String stationName) {
        return new QueryReducer();
    }
    private static class QueryReducer extends Reducer<Long, Long> {
        private long sum;
        @Override
        public void beginReduce() {
            sum = 0;
        }
        @Override
        public void reduce(Long isMember) {
            sum = sum + isMember;
        }
        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
