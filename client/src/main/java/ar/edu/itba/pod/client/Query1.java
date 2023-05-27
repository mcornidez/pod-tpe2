package ar.edu.itba.pod.client;

import ar.edu.itba.pod.BikeRent;
import ar.edu.itba.pod.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.opencsv.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;


public class Query1 {
    private static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ParseException {
        // -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [params]
        List<String> addresses;
        try {
            addresses = Arrays.asList(ClientUtils.getProperty(ClientUtils.ADDRESSES, () -> "Missing address/addresses.", x -> x).get().split(";"));
        } catch (NoSuchElementException e){
            throw new RuntimeException();
        }
        final String inPath = ClientUtils.getProperty(ClientUtils.IN_PATH, () -> "Missing in path.", x -> x).orElseThrow();
        final String outPath = ClientUtils.getProperty(ClientUtils.OUT_PATH, () -> "Missing out path.", x -> x).orElseThrow();

        logger.info("tpe2-g2 Query1 Starting ...");

        logger.info("Starting bikes parsing...");
        final LogManager logManager = new LogManager(outPath, "time1.txt");
        logManager.writeLog(
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                Query1.class.getName(),
                Thread.currentThread().getStackTrace()[1].getLineNumber(),
                "Inicio de la lectura del archivo"
        );
        List<String[]> bikesArray = ClientUtils.getCSVData(inPath + "/bikes.csv");
        List<BikeRent> bikes = ClientUtils.parseBikes(bikesArray);
        logger.info("Ended bikes parsing. Read {} bikes", bikes.size());
        logger.info("Starting stations parsing...");
        List<String[]> stationsArray = ClientUtils.getCSVData(inPath + "/stations.csv");
        List<Station> stations = ClientUtils.parseStations(stationsArray);
        logger.info("Ended stations parsing. Read {} stations", stations.size());
        logManager.writeLog(
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                Query1.class.getName(),
                Thread.currentThread().getStackTrace()[1].getLineNumber(),
                "Fin de lectura del archivo"
        );

        logger.info("Hazelcast client Starting...");
        HazelcastInstance hazelcastInstance = ClientUtils.getHazelClientInstance(addresses);
        logger.info("Hazelcast client started");

        IList<Station> stationsIList = hazelcastInstance.getList("stations");
        stationsIList.clear();
        stationsIList.addAll(stations);
        logger.info("Total stations: " + hazelcastInstance.getList("sensors").size());
        IList<BikeRent> bikesIList = hazelcastInstance.getList("bikes");
        bikesIList.clear();
        bikesIList.addAll(bikes);
        logger.info("Total bikes: " + hazelcastInstance.getList("bikes").size());

    }


}
