package ar.edu.itba.pod.models;

import java.io.Serializable;

public class Station implements Serializable {

    private final Integer pk;
    private final String name;
    private final Double latitude;
    private final Double longitude;

    public Station(Integer pk, String name, Double latitude, Double longitude) {
        this.pk = pk;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Integer getPk() {
        return pk;
    }

    public String getName() {
        return name;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

}
