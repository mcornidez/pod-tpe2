package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Journey;
import ar.edu.itba.pod.models.Station;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query2Mapper implements Mapper<String, BikeRent, String, Journey> {
    private final Map<Integer, Station> stations = new HashMap<>();

    public Query2Mapper(List<Station> stations) {
        for (Station s : stations){
            this.stations.put(s.getPk(), s);
        }
    }

    @Override
    public void map(String listName, BikeRent bikeRent, Context<String, Journey> context) {
        if (stations.containsKey(bikeRent.getEmplacement_pk_start())) {
            String stationName = stations.get(bikeRent.getEmplacement_pk_start()).getName();
            Station startStation = stations.get(bikeRent.getEmplacement_pk_start());
            Station endStation = stations.get(bikeRent.getEmplacement_pk_end());
            context.emit(stationName, new Journey(bikeRent.getStartDate(), bikeRent.getEndDate(), startStation, endStation));
        }
    }
}