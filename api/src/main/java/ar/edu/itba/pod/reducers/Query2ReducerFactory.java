package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.models.Journey;
import ar.edu.itba.pod.models.Station;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.time.LocalDate;
import java.util.Date;

public class Query2ReducerFactory implements ReducerFactory<String, Journey, Journey> {
    @Override
    public Reducer<Journey, Journey> newReducer(String stationName) {
        return new QueryReducer();
    }
    private static class QueryReducer extends Reducer<Journey, Journey> {
        private Journey fastestJourney;
        @Override
        public void beginReduce() {
            Station aux = new Station(1, "test", 0.0, 0.0);
            fastestJourney = new Journey(new Date(), new Date(), aux, aux);
        }
        @Override
        public void reduce(Journey journey) {
            if (fastestJourney.getSpeed() < journey.getSpeed()){
                fastestJourney = journey;
            }
        }

        @Override
        public Journey finalizeReduce() {
            return fastestJourney;
        }
    }
}