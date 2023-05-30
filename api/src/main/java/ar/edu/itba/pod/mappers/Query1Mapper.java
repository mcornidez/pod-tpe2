package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Station;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query1Mapper implements Mapper<String, BikeRent, String, Long> {
    private final Map<Integer, String> stations = new HashMap<>();

    public Query1Mapper(List<Station> stations) {
        for (Station s : stations){
            this.stations.put(s.getPk(), s.getName());
        }
    }

    @Override
    public void map(String listName, BikeRent bikeRent, Context<String, Long> context) {
        if (stations.containsKey(bikeRent.getEmplacement_pk_start())) {
            String stationName = stations.get(bikeRent.getEmplacement_pk_start());
            context.emit(stationName, Long.valueOf(bikeRent.getIs_member()));
        }
    }
}
