package io.substrait.isthmus;

import io.substrait.expression.proto.FunctionLookup;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

public class ProtoRelConverterTest extends PlanTestBase {
    @Test
    public void aggregate() throws IOException, SqlParseException {
        SqlToSubstrait s = new SqlToSubstrait();
        String[] values = asString("tpch/schema.sql").split(";");
        var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
        Plan p = s.execute("select count(L_ORDERKEY),sum(L_ORDERKEY) from lineitem ", creates);
        SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
        FunctionLookup functionLookup = FunctionLookup.builder().from(p).build();
        ProtoRelConverter relConverter = new ProtoRelConverter(functionLookup, extensionCollection, null);
        for (PlanRel planRel : p.getRelationsList()) {
            relConverter.from(planRel.getRoot().getInput());
        }
    }

    @Test
    public void filter() throws IOException, SqlParseException {
        SqlToSubstrait s = new SqlToSubstrait();
        String[] values = asString("tpch/schema.sql").split(";");
        var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
        Plan p = s.execute("select L_ORDERKEY from lineitem WHERE L_ORDERKEY + 1 > 10", creates);
        SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
        FunctionLookup functionLookup = FunctionLookup.builder().from(p).build();
        ProtoRelConverter relConverter = new ProtoRelConverter(functionLookup, extensionCollection, null);
        for (PlanRel planRel : p.getRelationsList()) {
            relConverter.from(planRel.getRoot().getInput());
        }
    }

    @Test
    public void joinAggSortLimit() throws IOException, SqlParseException {
        SqlToSubstrait s = new SqlToSubstrait();
        String[] values = asString("tpch/schema.sql").split(";");
        var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
        Plan p = s.execute("select\n" +
                "  l.l_orderkey,\n" +
                "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n" +
                "  o.o_orderdate,\n" +
                "  o.o_shippriority\n" +
                "\n" +
                "from\n" +
                "  \"customer\" c,\n" +
                "  \"orders\" o,\n" +
                "  \"lineitem\" l\n" +
                "\n" +
                "where\n" +
                "  c.c_mktsegment = 'HOUSEHOLD'\n" +
                "  and c.c_custkey = o.o_custkey\n" +
                "  and l.l_orderkey = o.o_orderkey\n" +
                "  and o.o_orderdate < date '1995-03-25'\n" +
                "  and l.l_shipdate > date '1995-03-25'\n" +
                "\n" +
                "group by\n" +
                "  l.l_orderkey,\n" +
                "  o.o_orderdate,\n" +
                "  o.o_shippriority\n" +
                "order by\n" +
                "  revenue desc,\n" +
                "  o.o_orderdate\n" +
                "limit 10", creates);
        SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
        FunctionLookup functionLookup = FunctionLookup.builder().from(p).build();
        ProtoRelConverter relConverter = new ProtoRelConverter(functionLookup, extensionCollection, null);
        for (PlanRel planRel : p.getRelationsList()) {
            relConverter.from(planRel.getRoot().getInput());
        }
    }
}
