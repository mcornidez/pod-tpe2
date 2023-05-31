package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Station;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ClientUtils {

    private final static Logger logger = LoggerFactory.getLogger(ClientUtils.class);

    public static final String ADDRESSES = "addresses";
    public static final String IN_PATH = "inPath";
    public static final String OUT_PATH = "outPath";
    public static final String N = "n";

    public static <T> Optional<T> getProperty(String name, Supplier<String> errorMsg, Function<String, T> converter){
        final String prop = System.getProperty(name);
        if(prop == null){
            final String msg = errorMsg.get();
            logger.error(msg);
            return Optional.empty();
        }
        try {
            return Optional.of(converter.apply(prop));
        }catch (ClassCastException e){
            logger.error("Cannot convert " + prop + " for " + name);
            System.out.println("Invalid argument " + prop + " for " + name);
        }
        return Optional.empty();
    }

    public static HazelcastInstance getHazelClientInstance(List<String> addresses) {
        String name = "tpe2-g2";
        String pass = "tpe2-g2-pass";

        ClientConfig clientConfig = new ClientConfig();

        GroupConfig groupConfig = new GroupConfig().setName(name).setPassword(pass);
        clientConfig.setGroupConfig(groupConfig);


        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig().setAddresses(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public static List<BikeRent> parseBikes (List<String> bikes){
        List<BikeRent> bikesList = new ArrayList<>();
        try {
            for (String s : bikes) {
                String[] data = s.split(";");
                Date start_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data[0]);
                Date end_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data[2]);
                Integer emplacement_pk_start = Integer.parseInt(data[1]);
                Integer emplacement_pk_end = Integer.parseInt(data[3]);
                Integer is_member = Integer.parseInt(data[4]);
                BikeRent bike = new BikeRent(start_date, end_date, emplacement_pk_start, emplacement_pk_end, is_member);
                bikesList.add(bike);
            }
        } catch (ParseException e){
            logger.error("Error parsing bikes.csv");
        }
        return bikesList;
    }

    public static List<Station> parseStations(List<String> stations){
        List<Station> stationsList = new ArrayList<>();
        for (String s : stations){
            String[] data = s.split(";");
            Integer pk = Integer.parseInt(data[0]);
            String name = data[1];
            Double latitude = Double.parseDouble(data[2]);
            Double longitude = Double.parseDouble(data[3]);
            Station station = new Station(pk, name, latitude, longitude);
            stationsList.add(station);
        }
        return stationsList;
    }

    public static List<String> getCSVData(String inPath) throws IOException {
        return Files.readAllLines(Path.of(inPath))
                .stream()
                .skip(1)
                .map(String::new)
                .toList();

    }
    public static void fillBikesIList(IList<BikeRent> bikesIList, String inPath) throws ParseException{
        List<BikeRent> aux = new ArrayList<>();
        int i = 0;
        final int BATCH_SIZE = 500000;
        do {
            aux.clear();
            try (Stream<String> lines = Files.lines(Path.of(inPath))) {
                lines.skip(1 + (long) i *BATCH_SIZE).limit(BATCH_SIZE)
                        .map(l -> l.split(";"))
                        .map(data -> {
                            try {
                                return new BikeRent(
                                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data[0]),
                                                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data[2]),
                                                Integer.parseInt(data[1]),
                                                Integer.parseInt(data[3]),
                                                Integer.parseInt(data[4])
                                );
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .forEach(aux::add);
                i++;
                bikesIList.addAll(aux);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        while (aux.size() == BATCH_SIZE);
    }
}
