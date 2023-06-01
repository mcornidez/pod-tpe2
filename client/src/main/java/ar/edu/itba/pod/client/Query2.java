package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.Query2Collator;
import ar.edu.itba.pod.combiners.Query2CombinerFactory;
import ar.edu.itba.pod.mappers.Query2Mapper;
import ar.edu.itba.pod.models.BikeRent;
import ar.edu.itba.pod.models.Journey;
import ar.edu.itba.pod.models.Station;
import ar.edu.itba.pod.reducers.Query2ReducerFactory;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

public class Query2 {
    private static Logger logger = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) {
        // -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX -DoutPath=YY [params]
        String addressesString = ClientUtils.getProperty(ClientUtils.ADDRESSES, () -> "Missing address/addresses.", x -> x).orElseThrow();
        final List<String> addresses = Arrays.asList(addressesString.split(";"));
        final String inPath = ClientUtils.getProperty(ClientUtils.IN_PATH, () -> "Missing in path.", x -> x).orElseThrow();
        final String outPath = ClientUtils.getProperty(ClientUtils.OUT_PATH, () -> "Missing out path.", x -> x).orElseThrow();
        final int n = Integer.parseInt(ClientUtils.getProperty(ClientUtils.N, () -> "Missing n.", x -> x).orElseThrow());

        logger.info("Query2 Starting ...");

        final LogManager logManager = new LogManager(outPath, "/time2.txt");
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
            IList<BikeRent> bikesIList = hazelcastInstance.getList("g2-query2-bikes-list");
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

            //Without combiner
            List<Map.Entry<String, Journey>> result = job
                    .mapper(new Query2Mapper(stations))
                    .reducer(new Query2ReducerFactory())
                    .submit(new Query2Collator())
                    .get();

/*
            //With combiner
            List<Map.Entry<String, Journey>> result = job
                    .mapper(new Query2Mapper(stations))
                    .combiner(new Query2CombinerFactory())
                    .reducer(new Query2ReducerFactory())
                    .submit(new Query2Collator())
                    .get();


 */


            logManager.writeLog(
                    Thread.currentThread().getStackTrace()[1].getMethodName(),
                    Query1.class.getName(),
                    Thread.currentThread().getStackTrace()[1].getLineNumber(),
                    "Fin del trabajo map/reduce"
            );

            logger.info("Fin del trabajo map/reduce");

            writeResultToFile(outPath + "/query2.csv", result, n);


        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.toString());
        } catch (NoSuchElementException e){
            logger.error("Failed to obtain addresses");
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        } finally {
            HazelcastClient.shutdownAll();
            logger.info("Finished query 2");
        }

    }

    static void writeResultToFile(String path, List<Map.Entry<String, Journey>> result, int n){
        try {
            BufferedWriter buffer = new BufferedWriter(new FileWriter(path, false));
            System.out.println("start_station;end_station;start_date;end_date;distance;speed");
            buffer.write("start_station;end_station;start_date;end_date;distance;speed");

            List<Map.Entry<String, Journey>> croppedResults = result.stream().limit(n).toList();

            for (Map.Entry<String, Journey> res : croppedResults){
                buffer.newLine();
                System.out.println();
                buffer.write(res.getKey() + ";" + res.getValue().toString());
                System.out.println(res.getKey() + ";" + res.getValue().toString());
            }
            buffer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
