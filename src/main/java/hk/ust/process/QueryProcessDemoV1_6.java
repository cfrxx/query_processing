package hk.ust.process;

import hk.ust.bean.*;
import hk.ust.functions.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author lyx
 * @version 1.6
 * 注意：需要根据 关联条件进行 keyby，才能保证 key相同的数据到一起去，才能匹配上
 *
 */
public class QueryProcessDemoV1_6 {

    public static void main(String[] args) throws Exception {
        // TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.读取数据
        // TODO 2.1 customer
        DataStreamSource<String> customerText = env.readTextFile("input/scale_factor_0.05/customer_updated.tbl");
        // TODO 2.2 nation
        DataStreamSource<String> nationText = env.readTextFile("input/scale_factor_0.05/nation_updated.tbl");
        // TODO 2.3 order
        DataStreamSource<String> orderText = env.readTextFile("input/scale_factor_0.05/orders_updated.tbl");
        // TODO 2.4 supplier
        DataStreamSource<String> supplierText = env.readTextFile("input/scale_factor_0.05/supplier_updated.tbl");
        // TODO 2.5 lineItem
        DataStreamSource<String> lineItemText = env.readTextFile("input/scale_factor_0.05/lineitem_updated.tbl");

        // TODO 3.处理数据
        // TODO 3.1切分、转换
        SingleOutputStreamOperator<Customer> customer = customerText.map(new MapFunction<String, Customer>() {
            @Override
            public Customer map(String s) throws Exception {
                String[] items = s.split("\\|");
                return new Customer(items[0], Integer.valueOf(items[1]), items[2], items[3], Integer.valueOf(items[4]), items[5], Float.parseFloat(items[6]), items[7], items[8]);
            }
        });
        SingleOutputStreamOperator<Nation> nation = nationText.map(new MapFunction<String, Nation>() {
            @Override
            public Nation map(String s) throws Exception {
                String[] items = s.split("\\|");
                return new Nation(items[0], Integer.valueOf(items[1]), items[2], Integer.valueOf(items[3]), items[4]);
            }
        }).filter(new FilterFunction<Nation>() {
            @Override
            public boolean filter(Nation nation) throws Exception {
                return nation.getN_name().equals("ALGERIA") || nation.getN_name().equals("BRAZIL");
            }
        });

        SingleOutputStreamOperator<Order> order = orderText.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String s) throws Exception {
                String[] items = s.split("\\|");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                //7|3914|O|231037.28|1996-01-10|2-HIGH|Clerk#000000470|0|ly special requests |
                return new Order(items[0], Integer.valueOf(items[1]), Integer.valueOf(items[2]), items[3], Double.valueOf(items[4]), LocalDate.parse(items[5], formatter), items[6], items[7], Integer.valueOf(items[8]), items[9]);
            }
        });
        SingleOutputStreamOperator<Supplier> supplier = supplierText.map(new MapFunction<String, Supplier>() {
            @Override
            public Supplier map(String s) throws Exception {
                String[] items = s.split("\\|");
                return new Supplier(items[0], Integer.valueOf(items[1]), items[2], items[3], Integer.valueOf(items[4]), items[5], Double.valueOf(items[6]), items[7]);
            }
        });
        SingleOutputStreamOperator<LineItem> lineItem = lineItemText.map(new MapFunction<String, LineItem>() {
            @Override
            public LineItem map(String s) throws Exception {
                String[] items = s.split("\\|");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return new LineItem(items[0], Integer.valueOf(items[1]), Integer.valueOf(items[2]), Integer.valueOf(items[3]), Integer.valueOf(items[4]), Double.valueOf(items[5]), Double.valueOf(items[6]), Double.valueOf(items[7]), Double.valueOf(items[8]),
                        items[9], items[10], LocalDate.parse(items[11], formatter), LocalDate.parse(items[12], formatter), LocalDate.parse(items[13], formatter), items[14], items[15], items[16]);
            }
        }).filter(new FilterFunction<LineItem>() {
            @Override
            public boolean filter(LineItem lineItem) throws Exception {
                return lineItem.getL_shipDate().compareTo(LocalDate.of(1995,1,1))>=0&&
                        lineItem.getL_shipDate().compareTo(LocalDate.of(1996,12,31))<=0;
            }
        });

        long startTime = System.currentTimeMillis(); // 记录开始时间

        // TODO 4. result A = nation.connect(customer).connect(order)
        SingleOutputStreamOperator<NationJoinCustomerJoinOrderRows> ncoStream = nation.connect(customer)
                .keyBy(n -> n.getN_nationKey(), c -> c.getC_nationKey())
                .process(new NationAndCustomerImpl())
                .connect(order).keyBy(nc -> nc.getC_custKey(), o -> o.getO_custKey())
                .process(new NCandOrderImpl());

        // TODO 5. result B = nation.connect(Supplier).connect(lineItem)
        SingleOutputStreamOperator<NationJoinSupplierJoinLineItemRows> nsiStream = nation.connect(supplier)
                .keyBy(n -> n.getN_nationKey(), s -> s.getS_nationKey())
                .process(new NationAndSupplierImpl())
                .connect(lineItem)
                .keyBy(ns -> ns.getS_suppKey(), l -> l.getL_suppKey())
                .process(new NSandLineItemImpl());

        // TODO 6. Final result = resultA.connect(resultB)
        ncoStream.connect(nsiStream)
                .keyBy(nco -> nco.getO_orderKey(), nci -> nci.getL_orderKey())
                .process(new NCOJoinNSIImpl())
                .keyBy(new KeySelector<FinalJointResults, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> getKey(FinalJointResults results) throws Exception {
                        return Tuple3.of(results.getSupp_nation(), results.getCust_nation(), results.getYear());
                    }
                }).process(new FinalProcess())
                .filter(new FilterFunction<Tuple4<String, String, Integer, Double>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, Integer, Double> value) throws Exception {
                        return value.f0.equals("ALGERIA")&&value.f1.equals("BRAZIL") ||
                                value.f0.equals("BRAZIL")&&value.f1.equals("ALGERIA");
                    }
                })
                .print();
        env.execute();
        long endTime = System.currentTimeMillis(); // 记录结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}
