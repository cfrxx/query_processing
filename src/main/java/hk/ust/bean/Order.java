package hk.ust.bean;

import java.time.LocalDate;

public class Order {
    private String flag;
    private Integer o_orderKey;
    private Integer o_custKey;
    private String o_orderStatus;
    private Double o_totalPrice;
    private LocalDate o_date;
    private String o_orderPriority;
    private String o_clerk;
    private Integer o_shipPriority;
    private String o_comment;

    public Order() {
    }

    public Order(String flag, Integer o_orderKey, Integer o_custKey, String o_orderStatus, Double o_totalPrice, LocalDate o_date, String o_orderPriority, String o_clerk, Integer o_shipPriority, String o_comment) {
        this.flag = flag;
        this.o_orderKey = o_orderKey;
        this.o_custKey = o_custKey;
        this.o_orderStatus = o_orderStatus;
        this.o_totalPrice = o_totalPrice;
        this.o_date = o_date;
        this.o_orderPriority = o_orderPriority;
        this.o_clerk = o_clerk;
        this.o_shipPriority = o_shipPriority;
        this.o_comment = o_comment;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getO_orderKey() {
        return o_orderKey;
    }

    public void setO_orderKey(Integer o_orderKey) {
        this.o_orderKey = o_orderKey;
    }

    public Integer getO_custKey() {
        return o_custKey;
    }

    public void setO_custKey(Integer o_custKey) {
        this.o_custKey = o_custKey;
    }

    public String getO_orderStatus() {
        return o_orderStatus;
    }

    public void setO_orderStatus(String o_orderStatus) {
        this.o_orderStatus = o_orderStatus;
    }

    public Double getO_totalPrice() {
        return o_totalPrice;
    }

    public void setO_totalPrice(Double o_totalPrice) {
        this.o_totalPrice = o_totalPrice;
    }

    public LocalDate getO_date() {
        return o_date;
    }

    public void setO_date(LocalDate o_date) {
        this.o_date = o_date;
    }

    public String getO_orderPriority() {
        return o_orderPriority;
    }

    public void setO_orderPriority(String o_orderPriority) {
        this.o_orderPriority = o_orderPriority;
    }

    public String getO_clerk() {
        return o_clerk;
    }

    public void setO_clerk(String o_clerk) {
        this.o_clerk = o_clerk;
    }

    public Integer getO_shipPriority() {
        return o_shipPriority;
    }

    public void setO_shipPriority(Integer o_shipPriority) {
        this.o_shipPriority = o_shipPriority;
    }

    public String getO_comment() {
        return o_comment;
    }

    public void setO_comment(String o_comment) {
        this.o_comment = o_comment;
    }
}
