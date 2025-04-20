package org.apache.flink.table.examples.java.my;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;

import java.util.stream.Stream;

import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;
import static org.apache.flink.table.api.Expressions.$;

public class PaymentJoiningExample {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // Generate data with a unified schema
        Table orders = env.fromCall(OrderGenerator.class);
        Table payments = env.fromCall(PaymentGenerator.class);

        // Union orders and payments before
        // partitioning and passing them into the Joiner function
        Table joined = orders.unionAll(payments)
                .partitionBy($("orderId"))
                .process(Joiner.class);

        joined.execute().print();
    }

    // A unified event for all input tables.
    // One of the sides is al empty.
    public static class UnifiedEvent {
        public int orderId;
        public Order order;
        public Payment payment;

        public static UnifiedEvent of(int orderId, Order order, Payment payment) {
            UnifiedEvent unifiedEvent = new UnifiedEvent();
            unifiedEvent.orderId = orderId;
            unifiedEvent.order = order;
            unifiedEvent.payment = payment;
            return unifiedEvent;
        }
    }

    // A PTF that generates Orders
    public static class OrderGenerator extends ProcessTableFunction<UnifiedEvent> {
        public void eval() {
            Stream.of(
                            Order.of("Bob", 1000001, 23.46, "USD"),
                            Order.of("Bob", 1000021, 6.99, "USD"),
                            Order.of("Alice", 1000601, 0.79, "EUR"),
                            Order.of("Charly", 1000703, 100.60, "EUR")
                    )
                    .map(order -> UnifiedEvent.of(order.id, order, null))
                    .forEach(this::collect);
        }
    }

    // A PTF that generates Payments
    public static class PaymentGenerator extends ProcessTableFunction<UnifiedEvent> {
        public void eval() {
            Stream.of(
                            Payment.of(999997870, 1000001),
                            Payment.of(999997870, 1000001),
                            Payment.of(999993331, 1000021),
                            Payment.of(999994111, 1000601)
                    )
                    .map(payment -> UnifiedEvent.of(payment.orderId, null, payment))
                    .forEach(this::collect);
        }
    }

    // Order POJO
    public static class Order {
        public String userId;
        public int id;
        public double amount;
        public String currency;

        public static Order of(String userId, int id, double amount, String currency) {
            Order order = new Order();
            order.userId = userId;
            order.id = id;
            order.amount = amount;
            order.currency = currency;
            return order;
        }
    }

    // Payment POJO
    public static class Payment {
        public int id;
        public int orderId;

        public static Payment of(int id, int orderId) {
            Payment payment = new Payment();
            payment.id = id;
            payment.orderId = orderId;
            return payment;
        }
    }

    // Function that buffers one object of each side to find exactly one join result.
    // The function expects that a payment event enters within 1 hour.
    // Otherwise, state is discarded using TTL.
    public static class Joiner extends ProcessTableFunction<JoinResult> {
        public void eval(
                Context ctx,
                @StateHint(ttl = "1 hour") JoinResult seen,
                @ArgumentHint(TABLE_AS_SET) UnifiedEvent input
        ) {
            if (input.order != null) {
                if (seen.order != null) {
                    // skip duplicates
                    return;
                } else {
                    // wait for matching payment
                    seen.order = input.order;
                }
            } else if (input.payment != null) {
                if (seen.payment != null) {
                    // skip duplicates
                    return;
                } else {
                    // wait for matching order
                    seen.payment = input.payment;
                }
            }

            if (seen.order != null && seen.payment != null) {
                // Send out the final join result
                collect(seen);
            }
        }
    }

    // POJO for the output of Joiner
    public static class JoinResult {
        public Order order;
        public Payment payment;
    }
}
