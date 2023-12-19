package hk.ust.bean;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * The index on one relation R
 */
public class Index {
    // an index on L(R), using the primary key of R as the key
    private Map<Integer, Tuple> IL; // I(L(R))

    // For non-leaf R: An index on N(R), using the primary key of R as the key.
    private Map<Integer, Tuple> IN; // I(N(R))

    // counter s(t) need to be stored together with IN
    private Map<Integer, Integer> ST; // s(t)

    // For non-leaf R and each child Rc, an index on (pk(R), pk(Rc)), pk(Rc) as the key.
    private Map<Integer, List<Tuple2<Integer, Integer>>> IRC; // I(R, Rc)

    public Index() {
    }

    public Index(Map<Integer, Tuple> IL, Map<Integer, Tuple> IN, Map<Integer, Integer> ST, Map<Integer, List<Tuple2<Integer, Integer>>> IRC) {
        this.IL = IL;
        this.IN = IN;
        this.ST = ST;
        this.IRC = IRC;
    }

    public Map<Integer, Tuple> getIL() {
        return IL;
    }

    public void setIL(Map<Integer, Tuple> IL) {
        this.IL = IL;
    }

    public Map<Integer, Tuple> getIN() {
        return IN;
    }

    public void setIN(Map<Integer, Tuple> IN) {
        this.IN = IN;
    }

    public Map<Integer, Integer> getST() {
        return ST;
    }

    public void setST(Map<Integer, Integer> ST) {
        this.ST = ST;
    }

    public Map<Integer, List<Tuple2<Integer, Integer>>> getIRC() {
        return IRC;
    }

    public void setIRC(Map<Integer, List<Tuple2<Integer, Integer>>> IRC) {
        this.IRC = IRC;
    }
}
