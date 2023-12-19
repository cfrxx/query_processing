package hk.ust.bean;

import org.apache.flink.api.java.tuple.Tuple;

import java.sql.Types;

public class Customer{
    private String flag;
    private Integer c_custKey;
    private String c_name;
    private String c_address;
    private Integer c_nationKey;
    private String c_phone;
    private float c_acctBal;
    private String c_mktSegment;
    private String c_comment;

    public Customer() {
    }

    public Customer(String flag, Integer c_custKey, String c_name, String c_address, Integer c_nationKey, String c_phone, float c_acctBal, String c_mktSegment, String c_comment) {
        this.flag = flag;
        this.c_custKey = c_custKey;
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationKey = c_nationKey;
        this.c_phone = c_phone;
        this.c_acctBal = c_acctBal;
        this.c_mktSegment = c_mktSegment;
        this.c_comment = c_comment;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Integer getC_custKey() {
        return c_custKey;
    }

    public void setC_custKey(Integer c_custKey) {
        this.c_custKey = c_custKey;
    }

    public String getC_name() {
        return c_name;
    }

    public void setC_name(String c_name) {
        this.c_name = c_name;
    }

    public String getC_address() {
        return c_address;
    }

    public void setC_address(String c_address) {
        this.c_address = c_address;
    }

    public Integer getC_nationKey() {
        return c_nationKey;
    }

    public void setC_nationKey(Integer c_nationKey) {
        this.c_nationKey = c_nationKey;
    }

    public String getC_phone() {
        return c_phone;
    }

    public void setC_phone(String c_phone) {
        this.c_phone = c_phone;
    }

    public float getC_acctBal() {
        return c_acctBal;
    }

    public void setC_acctBal(float c_acctBal) {
        this.c_acctBal = c_acctBal;
    }

    public String getC_mktSegment() {
        return c_mktSegment;
    }

    public void setC_mktSegment(String c_mktSegment) {
        this.c_mktSegment = c_mktSegment;
    }

    public String getC_comment() {
        return c_comment;
    }

    public void setC_comment(String c_comment) {
        this.c_comment = c_comment;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "flag='" + flag + '\'' +
                ", c_custKey=" + c_custKey +
                ", c_name='" + c_name + '\'' +
                ", c_address='" + c_address + '\'' +
                ", c_nationKey=" + c_nationKey +
                ", c_phone='" + c_phone + '\'' +
                ", c_acctBal=" + c_acctBal +
                ", c_mktSegment='" + c_mktSegment + '\'' +
                ", c_comment='" + c_comment + '\'' +
                '}';
    }
}
