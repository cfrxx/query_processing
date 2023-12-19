package hk.ust.bean;

import java.time.LocalDate;

public class FinalJointResults {
    private String flag;
    private Integer l_orderKey;
    private Integer l_lineNumber;
    private String supp_nation;
    private String cust_nation;
    private Integer year;
    private Double volume;

    public FinalJointResults() {
    }

    public FinalJointResults(String flag, Integer l_orderKey, Integer l_lineNumber, String supp_nation, String cust_nation, Integer year, Double volume) {
        this.flag = flag;
        this.l_orderKey = l_orderKey;
        this.l_lineNumber = l_lineNumber;
        this.supp_nation = supp_nation;
        this.cust_nation = cust_nation;
        this.year = year;
        this.volume = volume;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getL_orderKey() {
        return l_orderKey;
    }

    public void setL_orderKey(Integer l_orderKey) {
        this.l_orderKey = l_orderKey;
    }

    public Integer getL_lineNumber() {
        return l_lineNumber;
    }

    public void setL_lineNumber(Integer l_lineNumber) {
        this.l_lineNumber = l_lineNumber;
    }

    public String getSupp_nation() {
        return supp_nation;
    }

    public void setSupp_nation(String supp_nation) {
        this.supp_nation = supp_nation;
    }

    public String getCust_nation() {
        return cust_nation;
    }

    public void setCust_nation(String cust_nation) {
        this.cust_nation = cust_nation;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "FinalJointResults{" +
                "flag='" + flag + '\'' +
                ", l_orderKey=" + l_orderKey +
                ", l_lineNumber=" + l_lineNumber +
                ", supp_nation='" + supp_nation + '\'' +
                ", cust_nation='" + cust_nation + '\'' +
                ", year=" + year +
                ", volume=" + volume +
                '}';
    }
}
