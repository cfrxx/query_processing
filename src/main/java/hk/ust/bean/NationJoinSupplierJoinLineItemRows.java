package hk.ust.bean;

import java.time.LocalDate;

public class NationJoinSupplierJoinLineItemRows {
    private String flag;
    private Integer l_suppKey;
    private Integer l_orderKey;
    private Integer l_lineNumber;
    private LocalDate l_shipDate;
    private Double l_extendedPrice;
    private Double l_discount;
    private Integer s_suppKey;
    private Integer s_nationKey;
    private String n_name;

    public NationJoinSupplierJoinLineItemRows() {
    }

    public NationJoinSupplierJoinLineItemRows(String flag, Integer l_suppKey, Integer l_orderKey, Integer l_lineNumber, LocalDate l_shipDate, Double l_extendedPrice, Double l_discount, Integer s_suppKey, Integer s_nationKey, String n_name) {
        this.flag = flag;
        this.l_suppKey = l_suppKey;
        this.l_orderKey = l_orderKey;
        this.l_lineNumber = l_lineNumber;
        this.l_shipDate = l_shipDate;
        this.l_extendedPrice = l_extendedPrice;
        this.l_discount = l_discount;
        this.s_suppKey = s_suppKey;
        this.s_nationKey = s_nationKey;
        this.n_name = n_name;
    }

    public Integer getL_lineNumber() {
        return l_lineNumber;
    }

    public void setL_lineNumber(Integer l_lineNumber) {
        this.l_lineNumber = l_lineNumber;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getL_suppKey() {
        return l_suppKey;
    }

    public void setL_suppKey(Integer l_suppKey) {
        this.l_suppKey = l_suppKey;
    }

    public Integer getL_orderKey() {
        return l_orderKey;
    }

    public void setL_orderKey(Integer l_orderKey) {
        this.l_orderKey = l_orderKey;
    }

    public LocalDate getL_shipDate() {
        return l_shipDate;
    }

    public void setL_shipDate(LocalDate l_shipDate) {
        this.l_shipDate = l_shipDate;
    }

    public Double getL_extendedPrice() {
        return l_extendedPrice;
    }

    public void setL_extendedPrice(Double l_extendedPrice) {
        this.l_extendedPrice = l_extendedPrice;
    }

    public Double getL_discount() {
        return l_discount;
    }

    public void setL_discount(Double l_discount) {
        this.l_discount = l_discount;
    }

    public Integer getS_suppKey() {
        return s_suppKey;
    }

    public void setS_suppKey(Integer s_suppKey) {
        this.s_suppKey = s_suppKey;
    }

    public Integer getS_nationKey() {
        return s_nationKey;
    }

    public void setS_nationKey(Integer s_nationKey) {
        this.s_nationKey = s_nationKey;
    }

    public String getN_name() {
        return n_name;
    }

    public void setN_name(String n_name) {
        this.n_name = n_name;
    }

    @Override
    public String toString() {
        return "NationJoinSupplierJoinLineItemRows{" +
                "flag='" + flag + '\'' +
                ", l_orderKey=" + l_orderKey +
                ", l_lineNumber=" + l_lineNumber +
                ", l_suppKey=" + l_suppKey +
                ", l_shipDate=" + l_shipDate +
                ", l_extendedPrice=" + l_extendedPrice +
                ", l_discount=" + l_discount +
                ", s_suppKey=" + s_suppKey +
                ", s_nationKey=" + s_nationKey +
                ", n_name='" + n_name + '\'' +
                '}';
    }
}
