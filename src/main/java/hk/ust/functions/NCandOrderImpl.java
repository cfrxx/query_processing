package hk.ust.functions;

import hk.ust.bean.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class NCandOrderImpl extends CoProcessFunction<NationJoinCustomerRows, Order, NationJoinCustomerJoinOrderRows> {
    // TODO 1 定义 NationJoinCustomerRows 和 order 的 state
    // 存储nation流的alive元组
    MapState<Integer, NationJoinCustomerRows> nationJoinCustomerRows;

    // order流的state
    // I(R, Rc)
    MapState<Integer, List<Tuple>> orderIRRc;
    // order中的的所有元组 I(R)
    MapState<Integer, Order> orderIR;
    // 非活动元组 I(N(R))
    MapState<Integer, Order> orderIN;
    // 活动元组 I(L(R))
    MapState<Integer, Order> orderIL;
    // 计数器 count s(t)
    MapState<Integer, Integer> orderCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        nationJoinCustomerRows = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, NationJoinCustomerRows>("nationJoinCustomerRows", Types.INT, TypeInformation.of(NationJoinCustomerRows.class)));
        orderIRRc = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, List<Tuple>>("orderIRRc", Types.INT, Types.LIST(Types.TUPLE())));
        orderIR = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Order>("orderIR", Types.INT, TypeInformation.of(Order.class)));
        orderIL = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Order>("orderIL", Types.INT, TypeInformation.of(Order.class)));
        orderIN = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Order>("orderIN", Types.INT, TypeInformation.of(Order.class)));
        orderCounter = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("orderCounter", Types.INT, Types.INT));
    }

    @Override
    public void processElement1(NationJoinCustomerRows nationJoinCustomerValue, CoProcessFunction<NationJoinCustomerRows, Order, NationJoinCustomerJoinOrderRows>.Context context, Collector<NationJoinCustomerJoinOrderRows> collector) throws Exception {
        Integer custKey = nationJoinCustomerValue.getC_custKey();

        // 更新I(L(R)), 如果是insert, 则 input 进 nationIL, 否则，先remove再input
        if (nationJoinCustomerRows.contains(custKey)){
            nationJoinCustomerRows.remove(custKey);
            nationJoinCustomerRows.put(custKey, nationJoinCustomerValue);
        } else {
            nationJoinCustomerRows.put(custKey, nationJoinCustomerValue);
        }
        //System.out.println(custKey + " : " + nationJoinCustomerRows.get(nationKey));

        // nationJoinCustomerValue保存后，应也更新其parent relation Order的状态
        // 更新counter s(t), 利用I(R, Rc)查找 order中对应的元组
        List<Tuple> orderTuples = orderIRRc.get(custKey);
        if (orderTuples != null) {
            for (Tuple tuple : orderTuples) {
                Integer key = tuple.getField(0);
                Integer num = orderCounter.get(key);
                // 不区分Insert delete, 尝试正常更新counter
                if (num < 1) {
                    orderCounter.remove(key);
                    orderCounter.put(key, 1);
                    //System.out.println(key + " : " + orderCounter.get(key));
                }

                // 正常更新customer的I(L(R))
                Integer newNum = orderCounter.get(key);
                if (orderIL.contains(key)) {
                    orderIL.remove(key);
                    orderIL.put(key, orderIR.get(key));
                    //System.out.println(key + " : " + orderIL.get(key));
                } else {
                    orderIL.put(key, orderIR.get(key));
                    //System.out.println(key + " : " + orderIL.get(key));
                }
                // System.out.println(key+ " : "+orderCounter.get(key));

            }
        }

        // 合并元组
        for (Integer orderKey : orderIL.keys()){
            Order orderTuple = orderIL.get(orderKey);
            String oFlag = orderTuple.getFlag();
            NationJoinCustomerRows nationJoinCustomerTuple = nationJoinCustomerRows.get(orderTuple.getO_custKey());
            String ncFlag = nationJoinCustomerTuple.getFlag();
            String flag = null;
            if (oFlag.equals("+") && ncFlag.equals("+")){
                flag = "+";
            } else {
                flag = "-";
            }

            NationJoinCustomerJoinOrderRows joinResult = new NationJoinCustomerJoinOrderRows(
                    flag,
                    nationJoinCustomerTuple.getC_custKey(),
                    nationJoinCustomerTuple.getC_nationKey(),
                    nationJoinCustomerTuple.getN_nationKey(),
                    nationJoinCustomerTuple.getN_name(),
                    orderTuple.getO_orderKey(),
                    orderTuple.getO_custKey()

            );
            collector.collect(joinResult);
        }
    }

    @Override
    public void processElement2(Order orderValue, CoProcessFunction<NationJoinCustomerRows, Order, NationJoinCustomerJoinOrderRows>.Context context, Collector<NationJoinCustomerJoinOrderRows> collector) throws Exception {
        Integer orderKey = orderValue.getO_orderKey();
        Integer childKey = orderValue.getO_custKey();
        String orderflag = orderValue.getFlag();

        //  更新I(R)
        if (orderIR.contains(orderKey)){
            orderIR.remove(orderKey);
            orderIR.put(orderKey, orderValue);
        } else {
            orderIR.put(orderKey, orderValue);
        }
        //System.out.println(orderKey + " : " + orderValue);
        // 设置counter s(t), 初始值为0
        orderCounter.put(orderKey, 0);

        //  更新I(R, Rc)
        if (orderIRRc.contains(childKey)) {
            List<Tuple> list = orderIRRc.get(childKey);
            Tuple tuple = Tuple2.of(orderKey, childKey);
            if (!list.contains(tuple)){
                list.add(Tuple2.of(orderKey, childKey));
                orderIRRc.remove(childKey);
                orderIRRc.put(childKey, list);
            }
        } else {
            List<Tuple> list = new ArrayList<>();
            list.add(Tuple2.of(orderKey, childKey));
            orderIRRc.put(childKey, list);
        }
        //System.out.println(childKey + " : " + orderIRRc.get(childKey).toString());

        // 更新counter s(t)
        Integer num = orderCounter.get(orderKey);
        if (nationJoinCustomerRows.contains(childKey)){
            orderCounter.remove(orderKey);
            orderCounter.put(orderKey, 1);
            //System.out.println(orderKey + " : " + orderCounter.get(orderKey));
        }
        //System.out.println(orderKey+ " : "+orderCounter.get(orderKey));

        // 更新I(L(R))
        Integer newNum = orderCounter.get(orderKey);
        if (newNum == 1 && !orderIL.contains(orderKey)){
            orderIL.put(orderKey, orderValue);
            //System.out.println(orderKey + " : " + orderIL.get(orderKey));
        }
        else if (newNum == 1 && orderIL.contains(orderKey)){//说明是delete的元素
            orderIL.remove(orderKey);
            orderIL.put(orderKey, orderValue);
            //System.out.println(customerKey + " : " + customerIL.get(customerKey));
        } else{
            orderIN.put(orderKey, orderValue);
        }

        // 合并元组
        for (Integer key : orderIL.keys()){
            Order orderTuple = orderIL.get(key);
            String oFlag = orderTuple.getFlag();
            NationJoinCustomerRows nationJoinCustomerTuple = nationJoinCustomerRows.get(orderTuple.getO_custKey());
            String ncFlag = nationJoinCustomerTuple.getFlag();
            String flag = null;
            if (oFlag.equals("+") && ncFlag.equals("+")){
                flag = "+";
            } else {
                flag = "-";
            }

            NationJoinCustomerJoinOrderRows joinResult = new NationJoinCustomerJoinOrderRows(
                    flag,
                    nationJoinCustomerTuple.getC_custKey(),
                    nationJoinCustomerTuple.getC_nationKey(),
                    nationJoinCustomerTuple.getN_nationKey(),
                    nationJoinCustomerTuple.getN_name(),
                    orderTuple.getO_orderKey(),
                    orderTuple.getO_custKey()

            );
            collector.collect(joinResult);
        }
    }


}
