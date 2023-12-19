package hk.ust.functions;

import hk.ust.bean.Customer;
import hk.ust.bean.Nation;
import hk.ust.bean.NationJoinCustomerRows;
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

public class NationAndCustomerImpl extends CoProcessFunction<Nation, Customer, NationJoinCustomerRows> {
    // TODO 1 定义state
    // 存储nation流的alive元组
    MapState<Integer, Nation> nationIL;

    // customer流的state
    // I(R, Rc)
    MapState<Integer, List<Tuple>> customerIRRc;
    // customer中的的所有元组 I(R)
    MapState<Integer, Customer> customerIR;
    // 非活动元组 I(N(R))
    MapState<Integer, Customer> customerIN;
    // 活动元组 I(L(R))
    MapState<Integer, Customer> customerIL;
    // 计数器 count s(t)
    MapState<Integer, Integer> customerCounter;

    // TODO: 2 初始化state
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        nationIL = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Nation>("nationIL", TypeInformation.of(Integer.class), TypeInformation.of(Nation.class)));
        customerIR = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Customer>("customerIR", TypeInformation.of(Integer.class), TypeInformation.of(Customer.class)));
        customerIL = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Customer>("customerIL", TypeInformation.of(Integer.class), TypeInformation.of(Customer.class)));
        customerIN = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Customer>("customerIN", TypeInformation.of(Integer.class), TypeInformation.of(Customer.class)));
        customerCounter = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("customerCounter", Types.INT, Types.INT));
        customerIRRc = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, List<Tuple>>("customerIRRc", Types.INT, Types.LIST(Types.TUPLE())));

    }
    // TODO 3 处理child流
    @Override
    public void processElement1(Nation nationValue, CoProcessFunction<Nation, Customer, NationJoinCustomerRows>.Context context, Collector<NationJoinCustomerRows> collector) throws Exception {
        Integer nationKey = nationValue.getN_nationKey();

        // TODO 3.1 更新 I(L(R))
        // 更新I(L(R)), 如果是insert, 则 input 进 nationIL, 否则，先remove再input
        if (nationIL.contains(nationKey)){
                nationIL.remove(nationKey);
                nationIL.put(nationKey, nationValue);
        } else {
            nationIL.put(nationKey, nationValue);
        }
        //System.out.println(nationKey + " : " + nationIL.get(nationKey));

        // TODO 3.2 来了新数据，应同步更新 parent relation的 counter和 I(L(R)), 利用I(R, Rc)查找 parent 中对应的元组
        List<Tuple> customerTuples = customerIRRc.get(nationKey);
        if (customerTuples != null){
            for (Tuple tuple : customerTuples){
                Integer key = tuple.getField(0);
                Integer num = customerCounter.get(key);
                if (num < 1) {
                    customerCounter.remove(key);
                    customerCounter.put(key, 1);
                    //System.out.println(key + " : " + customerCounter.get(key));
                }

                // 根据更新后的counter, 更新parent relation的I(L(R))
                Integer newNum = customerCounter.get(key);
                if (customerIL.contains(key)){
                    customerIL.remove(key);
                    customerIL.put(key, customerIR.get(key));
                    //System.out.println(key + " : " + customerIL.get(key));
                } else {
                    customerIL.put(key, customerIR.get(key));
                    //System.out.println(key + " : " + customerIL.get(key));
                }
                // System.out.println(key+ " : "+customerCounter.get(key));
            }
        }

        // 合并元组
        for (Integer cust : customerIL.keys()){
            Customer customerTuple = customerIL.get(cust);
            String cFlag = customerTuple.getFlag();
            Nation nationTuple = nationIL.get(customerTuple.getC_nationKey());
            String nFlag = nationTuple.getFlag();
            String flag = null;
            if (cFlag.equals("+") && nFlag.equals("+")){
                flag = "+";
            } else {
                flag = "-";
            }

            NationJoinCustomerRows nationJoinCustomerRows = new NationJoinCustomerRows(
                    flag,
                    customerTuple.getC_custKey(),
                    customerTuple.getC_nationKey(),
                    nationTuple.getN_nationKey(),
                    nationTuple.getN_name()
            );
            collector.collect(nationJoinCustomerRows);
        }
    }
    // TODO 4 处理parent流
    @Override
    public void processElement2(Customer customerValue, CoProcessFunction<Nation, Customer, NationJoinCustomerRows>.Context context, Collector<NationJoinCustomerRows> collector) throws Exception {
        Integer customerKey = customerValue.getC_custKey();
        Integer childKey = customerValue.getC_nationKey();
        String customerflag = customerValue.getFlag();

        // TODO 4.1 更新I(R)
        if (customerIR.contains(customerKey)){
            customerIR.remove(customerKey);
            customerIR.put(customerKey, customerValue);
        } else {
            customerIR.put(customerKey, customerValue);
        }
        //System.out.println(customerKey + " : " + customerValue);
        // 设置counter s(t), 初始值为0
        customerCounter.put(customerKey, 0);

        // TODO 4.2 更新I(R, Rc)
        if (customerIRRc.contains(childKey)) {
            List<Tuple> list = customerIRRc.get(childKey);
            Tuple tuple = Tuple2.of(customerKey, childKey);
            if (!list.contains(tuple)){
                list.add(Tuple2.of(customerKey, childKey));
                customerIRRc.remove(childKey);
                customerIRRc.put(childKey, list);
            }
        } else {
            List<Tuple> list = new ArrayList<>();
            list.add(Tuple2.of(customerKey, childKey));
            customerIRRc.put(childKey, list);
        }
        //System.out.println(childKey + " : " + customerIRRc.get(childKey).toString());

        // TODO 4.3 更新counter s(t)
        Integer num = customerCounter.get(customerKey);
        if (nationIL.contains(childKey)){
            customerCounter.remove(customerKey);
            customerCounter.put(customerKey, 1);
            //System.out.println(customerKey + " : " + customerCounter.get(customerKey));
        }
        //System.out.println(customerKey+ " : "+customerCounter.get(customerKey));

        // TODO 4.3 更新I(L(R))和I(N(R))
        Integer newNum = customerCounter.get(customerKey);
        if (newNum == 1 && !customerIL.contains(customerKey)){
            customerIL.put(customerKey, customerValue);
            //System.out.println(customerKey + " : " + customerIL.get(customerKey));
        }
        else if (newNum == 1 && customerIL.contains(customerKey)){//说明是delete的元素
            customerIL.remove(customerKey);
            customerIL.put(customerKey, customerValue);
            //System.out.println(customerKey + " : " + customerIL.get(customerKey));
        } else{
            customerIN.put(customerKey, customerValue);
        }

        // 合并元组
        for (Integer cust : customerIL.keys()){
            Customer customerTuple = customerIL.get(cust);
            String cFlag = customerTuple.getFlag();
            Nation nationTuple = nationIL.get(customerTuple.getC_nationKey());
            String nFlag = nationTuple.getFlag();
            String flag = null;
            if (cFlag.equals("+") && nFlag.equals("+")){
                flag = "+";
            } else {
                flag = "-";
            }

            NationJoinCustomerRows nationJoinCustomerRows = new NationJoinCustomerRows(
                    flag,
                    customerTuple.getC_custKey(),
                    customerTuple.getC_nationKey(),
                    nationTuple.getN_nationKey(),
                    nationTuple.getN_name()
            );

            collector.collect(nationJoinCustomerRows);
        }
    }
}
