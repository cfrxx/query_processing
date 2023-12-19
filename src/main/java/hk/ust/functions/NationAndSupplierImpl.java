package hk.ust.functions;

import hk.ust.bean.Supplier;
import hk.ust.bean.Nation;
import hk.ust.bean.NationJoinSupplierRows;
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

public class NationAndSupplierImpl extends CoProcessFunction<Nation, Supplier, NationJoinSupplierRows> {
    // TODO 1 定义state
    // 存储child流的alive元组
    MapState<Integer, Nation> childIL;

    // parent流的state
    // I(R, Rc)
    MapState<Integer, List<Tuple>> parentIRRc;
    // parent中的的所有元组 I(R)
    MapState<Integer, Supplier> parentIR;
    // 非活动元组 I(N(R))
    MapState<Integer, Supplier> parentIN;
    // 活动元组 I(L(R))
    MapState<Integer, Supplier> parentIL;
    // 计数器 count s(t)
    MapState<Integer, Integer> parentCounter;

    // TODO: 2 初始化state
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        childIL = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Nation>("childIL", TypeInformation.of(Integer.class), TypeInformation.of(Nation.class)));
        parentIR = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Supplier>("parentIR", TypeInformation.of(Integer.class), TypeInformation.of(Supplier.class)));
        parentIL = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Supplier>("parentIL", TypeInformation.of(Integer.class), TypeInformation.of(Supplier.class)));
        parentIN = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Supplier>("parentIN", TypeInformation.of(Integer.class), TypeInformation.of(Supplier.class)));
        parentCounter = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("parentCounter", Types.INT, Types.INT));
        parentIRRc = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, List<Tuple>>("parentIRRc", Types.INT, Types.LIST(Types.TUPLE())));

    }
    // TODO 3 处理child流
    @Override
    public void processElement1(Nation childValue, CoProcessFunction<Nation, Supplier, NationJoinSupplierRows>.Context context, Collector<NationJoinSupplierRows> collector) throws Exception {
        Integer childKey = childValue.getN_nationKey();
        String childFlag = childValue.getFlag();

        // TODO 3.1 更新 I(L(R))
        // 更新I(L(R)), 如果是insert, 则 input 进 childIL, 否则，先remove再input
        if (childIL.contains(childKey)){
                childIL.remove(childKey);
                childIL.put(childKey, childValue);
        } else {
            childIL.put(childKey, childValue);
        }
        //System.out.println(childKey + " : " + childIL.get(childKey));

        // TODO 3.2 来了新数据，应同步更新 parent relation的 counter和 I(L(R)), 利用I(R, Rc)查找 parent 中对应的元组
        List<Tuple> parentTuples = parentIRRc.get(childKey);
        if (parentTuples != null){
            for (Tuple tuple : parentTuples){
                Integer key = tuple.getField(0);

                // TODO 3.2.1 更新 parent 的 counter
                Integer num = parentCounter.get(key);
                if (num < 1) {
                    parentCounter.remove(key);
                    parentCounter.put(key, 1);
                    //System.out.println(key + " : " + parentCounter.get(key));
                }
                // TODO 3.2.2 更新 parent 的 I(L(R))
                Integer newNum = parentCounter.get(key);
                if (parentIL.contains(key)){
                    parentIL.remove(key);
                    parentIL.put(key, parentIR.get(key));
                    //System.out.println(key + " : " + parentIL.get(key));
                } else {
                    parentIL.put(key, parentIR.get(key));
                    //System.out.println(key + " : " + parentIL.get(key));
                }
                // System.out.println(key+ " : "+parentCounter.get(key));

                // TODO 3.2.3 parent JOIN child using I(R, Rc)
                Supplier parentValue = parentIL.get(key);
                String parentFlag = parentValue.getFlag();
                String flag = (childFlag.equals("+") && parentFlag.equals("+")) ? "+" : "-";
                NationJoinSupplierRows jointResults = new NationJoinSupplierRows(
                        flag,
                        parentValue.getS_suppKey(),
                        parentValue.getS_nationKey(),
                        childValue.getN_nationKey(),
                        childValue.getN_name()
                );
                collector.collect(jointResults);
            }
        }
    }
    // TODO 4 处理parent流
    @Override
    public void processElement2(Supplier parentValue, CoProcessFunction<Nation, Supplier, NationJoinSupplierRows>.Context context, Collector<NationJoinSupplierRows> collector) throws Exception {
        Integer parentKey = parentValue.getS_suppKey();
        Integer childKey = parentValue.getS_nationKey();
        String parentFlag = parentValue.getFlag();

        // TODO 4.1 更新I(R)
        if (parentIR.contains(parentKey)){
            parentIR.remove(parentKey);
            parentIR.put(parentKey, parentValue);
        } else {
            parentIR.put(parentKey, parentValue);
        }
        //System.out.println(parentKey + " : " + parentValue);
        // 设置counter s(t), 初始值为0
        parentCounter.put(parentKey, 0);

        // TODO 4.2 更新I(R, Rc)
        if (parentIRRc.contains(childKey)) {
            List<Tuple> list = parentIRRc.get(childKey);
            Tuple tuple = Tuple2.of(parentKey, childKey);
            // 判断是否重复加入list，因为可能会有deletion的tuple,其id是重复的
            if (!list.contains(tuple)){
                list.add(Tuple2.of(parentKey, childKey));
                parentIRRc.remove(childKey);
                parentIRRc.put(childKey, list);
            }
        } else {
            List<Tuple> list = new ArrayList<>();
            list.add(Tuple2.of(parentKey, childKey));
            parentIRRc.put(childKey, list);
        }
        //System.out.println(childKey + " : " + parentIRRc.get(childKey).toString());

        // TODO 4.3 更新counter s(t)
        Integer num = parentCounter.get(parentKey);
        if (childIL.contains(childKey)){
            parentCounter.remove(parentKey);
            parentCounter.put(parentKey, 1);
            //System.out.println(parentKey + " : " + parentCounter.get(parentKey));
        }
        //System.out.println(parentKey+ " : "+parentCounter.get(parentKey));

        // TODO 4.3 更新 I(L(R))和 I(N(R))
        Integer newNum = parentCounter.get(parentKey);
        if (newNum == 1 && !parentIL.contains(parentKey)){
            parentIL.put(parentKey, parentValue);
            //System.out.println(parentKey + " : " + parentIL.get(parentKey));
        }
        else if (newNum == 1 && parentIL.contains(parentKey)){//说明是delete的元素
            parentIL.remove(parentKey);
            parentIL.put(parentKey, parentValue);
            //System.out.println(customerKey + " : " + customerIL.get(customerKey));

        } else{
            parentIN.put(parentKey, parentValue);
        }

        // TODO 4.4 parent JOIN child
        if (parentIL.contains(parentKey)){
            Nation childTuple = childIL.get(parentValue.getS_nationKey());
            String childFlag = childTuple.getFlag();
            String flag = (childFlag.equals("+") && parentFlag.equals("+")) ? "+" : "-";
            NationJoinSupplierRows jointResults = new NationJoinSupplierRows(
                    flag,
                    parentValue.getS_suppKey(),
                    parentValue.getS_nationKey(),
                    childTuple.getN_nationKey(),
                    childTuple.getN_name()
            );
            collector.collect(jointResults);
        }
    }
}
