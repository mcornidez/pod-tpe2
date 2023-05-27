package ar.edu.itba.pod.client;

import ar.edu.itba.pod.BikeRent;
import ar.edu.itba.pod.Station;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
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

    public static List<BikeRent> parseBikes (List<String[]> bikes) throws ParseException {
        List<BikeRent> bikesList = new ArrayList<>();
        for (String[] s : bikes){
            Date start_date = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS").parse(s[0]);
            Date end_date = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SS").parse(s[2]);
            Integer emplacement_pk_start = Integer.parseInt(s[1]);
            Integer emplacement_pk_end = Integer.parseInt(s[3]);
            Integer is_member = Integer.parseInt(s[4]);
            BikeRent bike = new BikeRent(start_date, end_date, emplacement_pk_start, emplacement_pk_end, is_member);
            bikesList.add(bike);
        }
        return bikesList;
    }

    public static List<Station> parseStations(List<String[]> stations) throws ParseException {
        List<Station> stationsList = new ArrayList<>();
        for (String[] s : stations){
            Integer pk = Integer.parseInt(s[0]);
            String name = s[1];
            Long latitude = Long.parseLong(s[2]);
            Long longitude = Long.parseLong(s[3]);
            Station station = new Station(pk, name, latitude, longitude);
            stationsList.add(station);
        }
        return stationsList;
    }

    public static List<String[]> getCSVData(String inPath) {
        FileReader filereader = null;
        try {
            filereader = new FileReader(inPath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found");
        }

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(';')
                .build();

        // create csvReader object and skip first Line
        try (CSVReader csvReader = new CSVReaderBuilder(filereader)
                .withSkipLines(1)
                .withCSVParser(parser)
                .build()) {
            return csvReader.readAll();
        } catch (IOException e) {
            throw new RuntimeException("Error reading file");
        }
    }
}
