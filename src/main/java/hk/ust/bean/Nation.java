package hk.ust.bean;

public class Nation {
    private String flag;
    private Integer n_nationKey;
    private String n_name;
    private Integer n_regionKey; //foreign key to Region
    private String n_comment;

    public Nation() {
    }

    public Nation(String flag, Integer n_nationKey, String n_name, Integer n_regionKey, String n_comment) {
        this.flag = flag;
        this.n_nationKey = n_nationKey;
        this.n_name = n_name;
        this.n_regionKey = n_regionKey;
        this.n_comment = n_comment;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
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

    public Integer getN_regionKey() {
        return n_regionKey;
    }

    public void setN_regionKey(Integer n_regionKey) {
        this.n_regionKey = n_regionKey;
    }

    public String getN_comment() {
        return n_comment;
    }

    public void setN_comment(String n_comment) {
        this.n_comment = n_comment;
    }


    @Override
    public String toString() {
        return "Nation{" +
                "flag='" + flag + '\'' +
                ", n_nationKey=" + n_nationKey +
                ", n_name='" + n_name + '\'' +
                ", n_regionKey=" + n_regionKey +
                ", n_comment='" + n_comment + '\'' +
                '}';
    }
}
