package ar.edu.itba.pod.combiners;

import ar.edu.itba.pod.models.Journey;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;


public class Query2CombinerFactory implements CombinerFactory<String, Journey, Journey> {
    @Override
    public Combiner<Journey, Journey> newCombiner(String integer) {
        return new QueryCombiner();
    }

    private static class QueryCombiner extends Combiner<Journey, Journey> {
        private Journey fastestJourney = null;

        @Override
        public  void combine(Journey journey) {
            if (fastestJourney == null || fastestJourney.getSpeed() < journey.getSpeed()){
                fastestJourney = journey;
            }
        }

        @Override
        public void reset() {
            fastestJourney = null;
        }

        @Override
        public Journey finalizeChunk() {
            return fastestJourney;
        }
    }
}
