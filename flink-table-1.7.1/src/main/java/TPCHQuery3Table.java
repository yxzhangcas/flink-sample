import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;

import java.sql.Date;

/*
此功能未经过实际运行测试
 */

public class TPCHQuery3Table {
    private static String lineItemPath = null;
    private static String customerPath = null;
    private static String ordersPath = null;
    private static String outputPath = null;
    private static boolean parseParameters(String[] args) {
        if (args.length == 4) {
            lineItemPath = args[0];
            customerPath = args[1];
            ordersPath = args[2];
            outputPath = args[3];
            return true;
        } else {
            return false;
        }
    }
    private static class LineItem {
        public long id;
        public double extdPrice;
        public double discount;
        public String shipDate;
        public LineItem() {}
        public LineItem(long id, double extdPrice, double discount, String shipDate) {
            this.id = id;
            this.extdPrice = extdPrice;
            this.discount = discount;
            this.shipDate = shipDate;
        }
        @Override
        public String toString() {
            return "LineItem{" +
                    "id=" + id +
                    ", extdPrice=" + extdPrice +
                    ", discount=" + discount +
                    ", shipDate='" + shipDate + '\'' +
                    '}';
        }
    }
    private static DataSet<LineItem> getLineItemDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(lineItemPath)
                .fieldDelimiter("|").includeFields("10000110001")
                .pojoType(LineItem.class, "id", "extdPrice", "discount", "shipDate");
    }
    private static class Customer {
        public long id;
        public String mktSegment;
        public Customer() {}
        public Customer(long id, String mktSegment) {
            this.id = id;
            this.mktSegment = mktSegment;
        }
        @Override
        public String toString() {
            return "Customer{" +
                    "id=" + id +
                    ", mktSegment='" + mktSegment + '\'' +
                    '}';
        }
    }
    private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(customerPath)
                .fieldDelimiter("|").includeFields("1000001")
                .pojoType(Customer.class, "id", "mktSegment");
    }
    private static class Order {
        public long orderId;
        public long custId;
        public String orderDate;
        public long shipPrio;
        public Order() {}
        public Order(long orderId, long custId, String orderDate, long shipPrio) {
            this.orderId = orderId;
            this.custId = custId;
            this.orderDate = orderDate;
            this.shipPrio = shipPrio;
        }
        @Override
        public String toString() {
            return "Order{" +
                    "orderId=" + orderId +
                    ", custId=" + custId +
                    ", orderDate='" + orderDate + '\'' +
                    ", shipPrio=" + shipPrio +
                    '}';
        }
    }
    private static DataSet<Order> getOrdersDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(ordersPath)
                .fieldDelimiter("|").includeFields("11001001")
                .pojoType(Order.class, "orderId", "custId", "orderDate", "shipPrio");
    }
    /*
    USAGE: TPCHQuery3Expression <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>
    */
    public static void main(String[] args) throws Exception {
        if (!parseParameters(args)) {
            return;
        }
        Date date = Date.valueOf("1995-03-12");
        //Environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //TODO 这里有个问题需要解决：如何在Java中表达Scala的 filter('orderDate.toDate < date)
        Table lineItems = tableEnv.fromDataSet(getLineItemDataSet(env)).filter("TO_DATE(orderDate) < date");
        Table customers = tableEnv.fromDataSet(getCustomerDataSet(env)).filter("mktSegment === AUTOMOBILE");
        Table orders = tableEnv.fromDataSet(getOrdersDataSet(env)).filter("TO_DATE(orderDate) < date");

        Table items = orders.join(customers).where("custId = id").select("orderId, orderDate, shipPrio")
                .join(lineItems).where("orderId = id").select("orderId, extdPrice * (1.0 - discount) as revenue, orderDate, shipPrio");
        Table result = items.groupBy("orderId, orderDate, shipPrio")
                .select("orderId, SUM(revenue) as revenue, orderDate, shipPrio")
                .orderBy("revenue DESC, orderDate ASC");

        //TODO result.writeAsCsv(outputPath, "\n", "|")
        tableEnv.connect(new FileSystem().path(outputPath)).withFormat(new Csv().fieldDelimiter("|"));
        env.execute();
    }
}
