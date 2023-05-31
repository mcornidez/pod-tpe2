package ar.edu.itba.pod.models;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Journey implements Serializable {

    private final Date startDate;
    private final Date endDate;
    private final Station emplacement_pk_start;
    private final Station emplacement_pk_end;

    private final Double distance;

    private final Double speed;

    public Journey(Date startDate, Date endDate, Station emplacement_pk_start, Station emplacement_pk_end) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.emplacement_pk_start = emplacement_pk_start;
        this.emplacement_pk_end = emplacement_pk_end;
        this.distance = distanceTo(emplacement_pk_start.getLatitude(), emplacement_pk_start.getLongitude(), emplacement_pk_end.getLatitude(), emplacement_pk_end.getLongitude());
        Double time = (endDate.getTime() - startDate.getTime())/3600.0;
        this.speed = this.distance/time;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public String getEmplacement_pk_end() {
        return emplacement_pk_end.getName();
    }

    public Double getDistance() {
        return distance;
    }

    public Double getSpeed() {
        return speed;
    }

    public Double distanceTo(double lat_1, double lon_1, double lat_2, double lon_2) {
        double latitudeDifference = Math.toRadians(lat_2 - lat_1);
        double longitudeDifference = Math.toRadians(lon_2 - lon_1);
        double SQRTSegment = Math.sqrt(
                Math.pow(
                        Math.sin(latitudeDifference / 2)
                        , 2
                ) + Math.cos(Math.toRadians(lat_1)) * Math.cos(Math.toRadians(lat_2)) * Math.pow(
                        Math.sin(longitudeDifference / 2)
                        , 2
                )
        );
        int EARTH_RADIUS = 6371;
        return 2 * EARTH_RADIUS * Math.asin(SQRTSegment);
    }

    public int compareTo(Journey value) {
        return this.speed.compareTo(value.getSpeed());
    }

    @Override
    public String toString() {
        return emplacement_pk_end.getName() + ";" +
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(startDate) + ";" +
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(endDate) + ";" +
                String.format("%.2f", distance) + ";" +
                String.format("%.2f", speed);
    }
}
