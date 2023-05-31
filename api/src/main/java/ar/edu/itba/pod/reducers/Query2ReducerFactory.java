package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.Journey;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2ReducerFactory implements ReducerFactory<String, Journey, Journey> {
    @Override
    public Reducer<Journey, Journey> newReducer(String stationName) {
        return new QueryReducer();
    }
    private static class QueryReducer extends Reducer<Journey, Journey> {
        private Journey fastestJourney;
        @Override
        public void beginReduce() {
            fastestJourney = null;
        }
        @Override
        public void reduce(Journey journey) {
            if (fastestJourney == null || fastestJourney.getSpeed() < journey.getSpeed()){
                fastestJourney = journey;
            }
        }

        @Override
        public Journey finalizeReduce() {
            return fastestJourney;
        }
    }
}