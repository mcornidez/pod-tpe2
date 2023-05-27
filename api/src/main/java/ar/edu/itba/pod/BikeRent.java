package ar.edu.itba.pod;

import java.io.Serializable;
import java.util.Date;

public class BikeRent implements Serializable {

    private final Date startDate;
    private final Date endDate;
    private final Integer emplacement_pk_start;
    private final Integer emplacement_pk_end;
    private final Integer is_member;

    public BikeRent(Date startDate, Date endDate, Integer emplacement_pk_start, Integer emplacement_pk_end, Integer is_member) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.emplacement_pk_start = emplacement_pk_start;
        this.emplacement_pk_end = emplacement_pk_end;
        this.is_member = is_member;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public Integer getEmplacement_pk_start() {
        return emplacement_pk_start;
    }

    public Integer getEmplacement_pk_end() {
        return emplacement_pk_end;
    }

    public Boolean getIs_member() {
        return is_member == 1;
    }
}
