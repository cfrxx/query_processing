package hk.ust.bean;

public class NationJoinCustomerRows {
    private String flag;
    // customer
    private Integer c_custKey;
    private Integer c_nationKey;
    // nation
    private Integer n_nationKey;
    private String n_name;

    public NationJoinCustomerRows() {
    }

    public NationJoinCustomerRows(String flag, Integer c_custKey, Integer c_nationKey, Integer n_nationKey, String n_name) {
        this.flag = flag;
        this.c_custKey = c_custKey;
        this.c_nationKey = c_nationKey;
        this.n_nationKey = n_nationKey;
        this.n_name = n_name;
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

    public Integer getC_nationKey() {
        return c_nationKey;
    }

    public void setC_nationKey(Integer c_nationKey) {
        this.c_nationKey = c_nationKey;
    }

    public Integer getN_nationKey() {
        return n_nationKey;
    }

    public void setN_nationKey(Integer n_nationKey) {
        this.n_nationKey = n_nationKey;
    }

    public String getN_name() {
        return n_name;
    }

    public void setN_name(String n_name) {
        this.n_name = n_name;
    }
}
