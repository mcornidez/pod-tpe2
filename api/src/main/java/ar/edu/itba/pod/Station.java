package ar.edu.itba.pod;

import java.io.Serializable;

public class Station implements Serializable {

    private final Integer pk;
    private final String name;
    private final Long latitude;
    private final Long longitude;

    public Station(Integer pk, String name, Long latitude, Long longitude) {
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

    public Long getLatitude() {
        return latitude;
    }

    public Long getLongitude() {
        return longitude;
    }
}
