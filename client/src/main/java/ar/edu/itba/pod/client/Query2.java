package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.Query1Collator;
import ar.edu.itba.pod.mappers.Query1Mapper;
import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Station;
import ar.edu.itba.pod.reducers.Query1ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query2 {
    private static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) {
        // -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [params]
        String addressesString = ClientUtils.getProperty(ClientUtils.ADDRESSES, () -> "Missing address/addresses.", x -> x).orElseThrow();
        List<String> addresses = Arrays.asList(addressesString.split(";"));
        final String inPath = ClientUtils.getProperty(ClientUtils.IN_PATH, () -> "Missing in path.", x -> x).orElseThrow();
        final String outPath = ClientUtils.getProperty(ClientUtils.OUT_PATH, () -> "Missing out path.", x -> x).orElseThrow();
        final Integer n = Integer.getInteger(ClientUtils.getProperty(ClientUtils.N, () -> "Missing results limit.", x -> x).orElseThrow());

        logger.info("tpe2-g2 Query2 Starting ...");

        logger.info("Starting bikes parsing...");
        final LogManager logManager = new LogManager(outPath, "time2.txt");
        logManager.writeLog(
                Thread.currentThread().getStackTrace()[1].getMethodName(),
                Query1.class.getName(),
                Thread.currentThread().getStackTrace()[1].getLineNumber(),
                "Inicio de la lectura del archivo"
        );
        try {
            List<String> bikesArray = ClientUtils.getCSVData(inPath + "/bikes.csv");
            List<BikeRent> bikes = ClientUtils.parseBikes(bikesArray);
            logger.info("Ended bikes parsing. Read {} bikes", bikes.size());
            logger.info("Starting stations parsing...");
            List<String> stationsArray = ClientUtils.getCSVData(inPath + "/stations.csv");
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

            logger.info("Total stations: " + hazelcastInstance.getList("sensors").size());
            IList<BikeRent> bikesIList = hazelcastInstance.getList("bikes");
            bikesIList.clear();
            bikesIList.addAll(bikes);
            logger.info("Total bikes: " + hazelcastInstance.getList("bikes").size());

            logManager.writeLog(
                    Thread.currentThread().getStackTrace()[1].getMethodName(),
                    Query1.class.getName(),
                    Thread.currentThread().getStackTrace()[1].getLineNumber(),
                    "Inicio del trabajo map/reduce"
            );

            KeyValueSource<String, BikeRent> keyValueSource = KeyValueSource.fromList(bikesIList);
            Job<String, BikeRent> job = hazelcastInstance.getJobTracker("g2_query2").newJob(keyValueSource);

            List<Map.Entry<String, Long>> result = job
                    .mapper(new Query1Mapper(stations))
                    .reducer(new Query1ReducerFactory())
                    .submit(new Query1Collator())
                    .get();

            logManager.writeLog(
                    Thread.currentThread().getStackTrace()[1].getMethodName(),
                    Query1.class.getName(),
                    Thread.currentThread().getStackTrace()[1].getLineNumber(),
                    "Fin del trabajo map/reduce"
            );

            writeResultToFile(outPath + "/query2.csv", result, n);



        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error on map/reduce");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            HazelcastClient.shutdownAll();
            logger.info("Finished query 2");
        }



    }

    static void writeResultToFile(String path, List<Map.Entry<String, Long>> result, int n){
        try {
            BufferedWriter buffer = new BufferedWriter(new FileWriter(path, false));
            buffer.write("start_station;end_station;start_date;end_date;distance;speed");

            List<Map.Entry<String, Long>> croppedResults = result.stream().limit(n).collect(Collectors.toList());

            for (Map.Entry<String, Long> res : croppedResults){
                buffer.newLine();
                buffer.write(res.getKey() + ";" + res.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
