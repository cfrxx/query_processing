package hk.ust.functions;
import hk.ust.bean.FinalJointResults;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FinalProcess extends ProcessFunction<FinalJointResults, Tuple4<String, String, Integer, Double>> {
    MapState<Tuple, Double> output;
    MapState<Tuple, FinalJointResults> jointResults;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        output = getRuntimeContext().getMapState(new MapStateDescriptor<>("output", Types.TUPLE(), Types.DOUBLE));
        jointResults = getRuntimeContext().getMapState(new MapStateDescriptor<>("jointResults", Types.TUPLE(), TypeInformation.of(FinalJointResults.class)));
    }
    @Override
    public void processElement(FinalJointResults value, ProcessFunction<FinalJointResults, Tuple4<String, String, Integer, Double>>.Context context, Collector<Tuple4<String, String, Integer, Double>> collector) throws Exception {
        Tuple lineItemKey = Tuple2.of(value.getL_orderKey(), value.getL_lineNumber());
        Tuple outKey = Tuple3.of(value.getSupp_nation(), value.getCust_nation(), value.getYear());
        String flag = value.getFlag();
        Double volume = value.getVolume();

        if (jointResults.contains(lineItemKey)){
            String oldFlag = jointResults.get(lineItemKey).getFlag();
            // 原来有某个元组，现在把它删掉
            if (oldFlag.equals("+") && flag.equals("-")){
                jointResults.remove(lineItemKey);
                jointResults.put(lineItemKey, value);
                // 如果output里有该key，就把该key的值取出来，减去当前value的volume，再input进去
                if (output.contains(outKey)){
                    Double sumVolume = output.get(outKey);
                    output.remove(outKey);
                    output.put(outKey, sumVolume - volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);

                } // 如果output里没有该key, 就把当前value的volume值input进去
                else {
                    output.put(outKey, volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);
                }

            }
            // 原来删掉了某个元组，现在又insert一个
            else if (oldFlag.equals("-") && flag.equals("+")) {
                jointResults.remove(lineItemKey);
                jointResults.put(lineItemKey, value);
                // 如果output里有该key，就把该key的值取出来，减去当前value的volume，再input进去
                if (output.contains(outKey)){
                    Double sumVolume = output.get(outKey);
                    output.remove(outKey);
                    output.put(outKey, sumVolume + volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);
                } // 如果output里没有该key, 就把当前value的volume值input进去
                else {
                    output.put(outKey, volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);
                }

            }
        } else {
            if (flag.equals("+")){
                jointResults.remove(lineItemKey);
                jointResults.put(lineItemKey, value);
                // 如果output里有该key，就把该key的值取出来，加上当前value的volume，再input进去
                if (output.contains(outKey)){
                    Double sumVolume = output.get(outKey);
                    output.remove(outKey);
                    output.put(outKey, sumVolume + volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);
                } // 如果output里没有该key, 就把当前value的volume值input进去
                else {
                    output.put(outKey, volume);
                    //collect
                    Tuple4<String, String, Integer, Double> t = Tuple4.of(outKey.getField(0), outKey.getField(1), outKey.getField(2), output.get(outKey));
                    collector.collect(t);
                }
            }
        }

//        for (Tuple key : output.keys()){
//            Tuple4<String, String, Integer, Double> t = Tuple4.of(key.getField(0), key.getField(1), key.getField(2), output.get(key));
//            collector.collect(t);
//        }
    }
}
