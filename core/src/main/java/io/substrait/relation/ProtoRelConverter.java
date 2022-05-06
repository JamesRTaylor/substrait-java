package io.substrait.relation;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.FunctionLookup;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.FromProto;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts from proto to pojo rel representation
 * TODO:
 * AdvancedExtension
 * Remap
 * Missing Rel subclasses: CrossJoin, Set, etc
 *
 */
public class ProtoRelConverter {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoRelConverter.class);

    private final FunctionLookup lookup;
    private final SimpleExtension.ExtensionCollection extensions;
    private final Type rootType;

    public ProtoRelConverter(FunctionLookup lookup, SimpleExtension.ExtensionCollection extensions, Type rootType) {
        this.lookup = lookup;
        this.extensions = extensions;
        this.rootType = rootType;
    }

    public Rel from(io.substrait.proto.Rel rel) {
        io.substrait.proto.Rel.RelTypeCase relType = rel.getRelTypeCase();
        switch (relType) {
            case READ -> {
                ReadRel readRel = rel.getRead();
                if (readRel.hasVirtualTable()) {
                    return newVirtualTable(readRel);
                } else if (readRel.hasNamedTable()) {
                    return newNamedScan(readRel);
                } else { // FIXME: EmptyScan?
                    return newEmptyScan(readRel);
                }
            }
            case FILTER -> {
                return newFilter(rel.getFilter());
            }
            case FETCH -> {
                return newFetch(rel.getFetch());
            }
            case AGGREGATE -> {
                return newAggregate(rel.getAggregate());
            }
            case SORT -> {
                return newSort(rel.getSort());
            }
            case JOIN -> {
                return newJoin(rel.getJoin());
            }
            case PROJECT -> {
                return newProject(rel.getProject());
            }
            default -> {
//                case SET:
//                case EXTENSION_SINGLE:
//                case EXTENSION_MULTI:
//                case EXTENSION_LEAF:
//                case CROSS:
//                case RELTYPE_NOT_SET:
                throw new UnsupportedOperationException();
            }
        }
    }

    private Filter newFilter(FilterRel filterRel) {
        Rel input = from(filterRel.getInput());
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
        Expression expression = expressionConverter.from(filterRel.getCondition());
        return Filter.builder().input(input).condition(expression).build();
    }

    private NamedStruct newNamedStruct(ReadRel readRel) {
        io.substrait.proto.NamedStruct protoNamedStruct = readRel.getBaseSchema();
        io.substrait.proto.Type.Struct protoStruct = protoNamedStruct.getStruct();
        List<Type> types = new ArrayList<>(protoStruct.getTypesCount());
        for (io.substrait.proto.Type protoType : protoStruct.getTypesList()) {
            Type type = FromProto.from(protoType);
            types.add(type);
        }
        // FIXME: put in util
        boolean isNullable = protoStruct.getNullability() == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE;
        Type.Struct struct = Type.Struct.builder().fields(types).nullable(isNullable).build();
        return ImmutableNamedStruct.builder().names(protoNamedStruct.getNamesList()).struct(struct).build();
    }

    private EmptyScan newEmptyScan(ReadRel readRel) {
        NamedStruct namedStruct = newNamedStruct(readRel);
        var builder = EmptyScan.builder().initialSchema(namedStruct);
        if (readRel.hasFilter()) {
            ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, rootType);
            Expression filter = expressionConverter.from(readRel.getFilter());
            builder.filter(filter);
        }
        return builder.build();
    }

    private NamedScan newNamedScan(ReadRel readRel) {
        NamedStruct namedStruct = newNamedStruct(readRel);
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, rootType);
        ImmutableNamedScan.Builder builder = NamedScan.builder()
                .initialSchema(namedStruct)
                // FIXME: Necessary since names are already in NamedStruct?
                .names(readRel.getBaseSchema().getNamesList());
        if (readRel.hasFilter()) {
            Expression filter = expressionConverter.from(readRel.getFilter());
            builder.filter(filter);
        }
        return builder.build();
    }

    private VirtualTableScan newVirtualTable(ReadRel readRel) {
        ReadRel.VirtualTable virtualTable = readRel.getVirtualTable();
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, rootType);
        Expression filter = expressionConverter.from(readRel.getFilter());
        List<Expression.StructLiteral> structLiterals = new ArrayList<>(virtualTable.getValuesCount());
        for (io.substrait.proto.Expression.Literal.Struct struct : virtualTable.getValuesList()) {
            List<Expression.Literal> literals = new ArrayList<>(struct.getFieldsCount());
            for (io.substrait.proto.Expression.Literal protoLiteral : struct.getFieldsList()) {
                // FIXME: Ok to expose this or better to create new util?
                Expression.Literal literal = expressionConverter.from(protoLiteral);
                literals.add(literal);
            }
            ImmutableExpression.StructLiteral structLiteral = ImmutableExpression.StructLiteral.builder().fields(literals).build();
            structLiterals.add(structLiteral);
        }
        return VirtualTableScan.builder().filter(filter).rows(structLiterals).build();
    }

    private Fetch newFetch(FetchRel fetchRel) {
        Rel input = from(fetchRel.getInput());
        return Fetch.builder()
                .input(input)
                .count(fetchRel.getCount())
                .offset(fetchRel.getOffset())
                .build();
    }

    private Project newProject(ProjectRel projectRel) {
        Rel input = from(projectRel.getInput());
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
        List<Expression> expressions = new ArrayList<>(projectRel.getExpressionsCount());
        for (io.substrait.proto.Expression protoExpression : projectRel.getExpressionsList()) {
            Expression expression = expressionConverter.from(protoExpression);
            expressions.add(expression);
        }
        ImmutableProject.Builder builder = Project.builder();
        return builder.input(input).expressions(expressions).build();
    }

    private Aggregate newAggregate(AggregateRel aggregateRel) {
        Rel input = from(aggregateRel.getInput());
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
        List<Aggregate.Grouping> groupings = new ArrayList<>(aggregateRel.getGroupingsCount());
        for (AggregateRel.Grouping protoGrouping : aggregateRel.getGroupingsList()) {
            List<Expression> expressions = new ArrayList<>(protoGrouping.getGroupingExpressionsCount());
            for (io.substrait.proto.Expression protoExpression : protoGrouping.getGroupingExpressionsList()) {
                Expression expression = expressionConverter.from(protoExpression);
                expressions.add(expression);
            }
            Aggregate.Grouping grouping = Aggregate.Grouping.builder().expressions(expressions).build();
            groupings.add(grouping);
        }
        List<Aggregate.Measure> measures = new ArrayList<>(aggregateRel.getMeasuresCount());
        for (AggregateRel.Measure protoMeasure : aggregateRel.getMeasuresList()) {
            AggregateFunction protoAggFunction = protoMeasure.getMeasure();
            List<Expression> arguments = new ArrayList<>(protoAggFunction.getArgsCount());
            for (io.substrait.proto.Expression protoExpression : protoAggFunction.getArgsList()) {
                Expression expression = expressionConverter.from(protoExpression);
                arguments.add(expression);
            }
            Type outputType = FromProto.from(protoAggFunction.getOutputType());
            int funcRef = protoAggFunction.getFunctionReference();
            SimpleExtension.AggregateFunctionVariant declaration = lookup.getAggregateFunction(funcRef, extensions);

            AggregateFunctionInvocation function = AggregateFunctionInvocation.builder()
                    .arguments(arguments)
                    .declaration(declaration)
                    .outputType(outputType)
                    .aggregationPhase(Expression.AggregationPhase.fromProto(protoAggFunction.getPhase()))
                    .build();
            ImmutableMeasure.Builder builder = Aggregate.Measure.builder();
            builder.function(function);
            if (protoMeasure.hasFilter()) {
                Expression filter = expressionConverter.from(protoMeasure.getFilter());
                builder.preMeasureFilter(filter);
            }
            Aggregate.Measure measure = builder.build();
            measures.add(measure);
        }
        return Aggregate.builder().input(input).groupings(groupings).measures(measures).build();
    }

    private Sort newSort(SortRel sortRel) {
        Rel input = from(sortRel.getInput());
        List<Expression.SortField> sortFields = new ArrayList<>(sortRel.getSortsCount());
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, input.getRecordType());
        for (SortField protoSortField : sortRel.getSortsList()) {
            Expression expression = expressionConverter.from(protoSortField.getExpr());
            Expression.SortField sortField = Expression.SortField.builder()
                    .direction(Expression.SortDirection.fromProto(protoSortField.getDirection()))
                    .expr(expression)
                    .build();
            sortFields.add(sortField);
        }
        return Sort.builder().input(input).sortFields(sortFields).build();
    }

    private Join newJoin(JoinRel joinRel) {
        Rel left = from(joinRel.getLeft());
        Rel right = from(joinRel.getRight());
        // TODO: rootType?
        ProtoExpressionConverter expressionConverter = new ProtoExpressionConverter(lookup, extensions, rootType);
        Expression condition = expressionConverter.from(joinRel.getExpression());
        Join.JoinType joinType = Join.JoinType.fromProto(joinRel.getType());
        var builder = Join.builder()
                .condition(condition)
                .joinType(joinType)
                .left(left)
                .right(right);
        if (joinRel.hasPostJoinFilter()) {
            Expression postFilter = expressionConverter.from(joinRel.getPostJoinFilter());
            builder.postJoinFilter(postFilter);
        }
        return builder.build();
    }
}
