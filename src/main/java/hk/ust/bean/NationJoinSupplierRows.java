package hk.ust.bean;

public class NationJoinSupplierRows {
    private String flag;
    private Integer s_suppKey;
    private Integer s_nationKey;
    private Integer n_nationKey;
    private String n_name;

    public NationJoinSupplierRows() {
    }

    public NationJoinSupplierRows(String flag, Integer s_suppKey, Integer s_nationKey, Integer n_nationKey, String n_name) {
        this.flag = flag;
        this.s_suppKey = s_suppKey;
        this.s_nationKey = s_nationKey;
        this.n_nationKey = n_nationKey;
        this.n_name = n_name;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
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

    @Override
    public String toString() {
        return "NationJoinSupplierRows{" +
                "flag='" + flag + '\'' +
                ", s_suppKey=" + s_suppKey +
                ", s_nationKey=" + s_nationKey +
                ", n_nationKey=" + n_nationKey +
                ", n_name='" + n_name + '\'' +
                '}';
    }
}
