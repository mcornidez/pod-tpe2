package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.Query1Collator;
import ar.edu.itba.pod.mappers.Query1Mapper;
import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Station;
import ar.edu.itba.pod.reducers.Query1ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Query1 {
    private static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) {
        // -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [params]
        String addressesString = ClientUtils.getProperty(ClientUtils.ADDRESSES, () -> "Missing address/addresses.", x -> x).orElseThrow();
        final List<String> addresses = Arrays.asList(addressesString.split(";"));
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
        try {

            logger.info("Hazelcast client Starting...");
            HazelcastInstance hazelcastInstance = ClientUtils.getHazelClientInstance(addresses);
            logger.info("Hazelcast client started");
            IList<BikeRent> bikesIList = hazelcastInstance.getList("g2-query1-bikes-list");
            bikesIList.clear();

            logger.info("Starting bikes parsing...");
            ClientUtils.fillBikesIList(bikesIList, inPath + "/bikes.csv");
            logger.info("Ended bikes parsing. Read {} bikes: ", bikesIList.size());

            logger.info("Starting stations parsing...");
            List<String> stationsArray = ClientUtils.getCSVData(inPath + "/stations.csv");
            List<Station> stations = ClientUtils.parseStations(stationsArray);
            logger.info("Ended stations parsing. Read {} stations", stations.size());

            logManager.writeLog(
                    Thread.currentThread().getStackTrace()[1].getMethodName(),
                    Query1.class.getName(),
                    Thread.currentThread().getStackTrace()[1].getLineNumber(),
                    "Inicio del trabajo map/reduce"
            );

            logger.info("Inicio del trabajo map/reduce");


            KeyValueSource<String, BikeRent> keyValueSource = KeyValueSource.fromList(bikesIList);
            Job<String, BikeRent> job = hazelcastInstance.getJobTracker("g2_query1").newJob(keyValueSource);

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

            logger.info("Fin del trabajo map/reduce");

            writeResultToFile(outPath + "/query1.csv", result);

        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.toString());
        } catch (NoSuchElementException e){
            logger.error("Failed to obtain addresses");
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        } finally {
            HazelcastClient.shutdownAll();
            logger.info("Finished query 1");
        }

    }

    static void writeResultToFile(String path, List<Map.Entry<String, Long>> result){
        try {
            BufferedWriter buffer = new BufferedWriter(new FileWriter(path, false));
            System.out.println("station;started_trips");
            buffer.write("station;started_trips");
            for (Map.Entry<String, Long> res : result){
                buffer.newLine();
                System.out.println();
                buffer.write(res.getKey() + ";" + res.getValue());
                System.out.println(res.getKey() + ";" + res.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
