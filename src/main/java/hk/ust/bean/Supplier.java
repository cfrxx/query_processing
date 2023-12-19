package hk.ust.bean;

public class Supplier {
    private String flag;
    private Integer s_suppKey;
    private String s_name;
    private String s_address;
    private Integer s_nationKey;
    private String s_phone;
    private Double s_acctBal;
    private String s_comment;

    public Supplier() {
    }

    public Supplier(String flag, Integer s_suppKey, String s_name, String s_address, Integer s_nationKey, String s_phone, double s_acctBal, String s_comment) {
        this.flag = flag;
        this.s_suppKey = s_suppKey;
        this.s_name = s_name;
        this.s_address = s_address;
        this.s_nationKey = s_nationKey;
        this.s_phone = s_phone;
        this.s_acctBal = s_acctBal;
        this.s_comment = s_comment;
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

    public String getS_name() {
        return s_name;
    }

    public void setS_name(String s_name) {
        this.s_name = s_name;
    }

    public String getS_address() {
        return s_address;
    }

    public void setS_address(String s_address) {
        this.s_address = s_address;
    }

    public Integer getS_nationKey() {
        return s_nationKey;
    }

    public void setS_nationKey(Integer s_nationKey) {
        this.s_nationKey = s_nationKey;
    }

    public String getS_phone() {
        return s_phone;
    }

    public void setS_phone(String s_phone) {
        this.s_phone = s_phone;
    }

    public double getS_acctBal() {
        return s_acctBal;
    }

    public void setS_acctBal(double s_acctBal) {
        this.s_acctBal = s_acctBal;
    }

    public String getS_comment() {
        return s_comment;
    }

    public void setS_comment(String s_comment) {
        this.s_comment = s_comment;
    }

    @Override
    public String toString() {
        return "Supplier{" +
                "flag='" + flag + '\'' +
                ", s_suppKey=" + s_suppKey +
                ", s_name='" + s_name + '\'' +
                ", s_address='" + s_address + '\'' +
                ", s_nationKey=" + s_nationKey +
                ", s_phone='" + s_phone + '\'' +
                ", s_acctBal=" + s_acctBal +
                ", s_comment='" + s_comment + '\'' +
                '}';
    }
}
