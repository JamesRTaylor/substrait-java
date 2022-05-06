package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public interface FunctionLookup {
  SimpleExtension.ScalarFunctionVariant getScalarFunction(int reference, SimpleExtension.ExtensionCollection extensions);
  SimpleExtension.AggregateFunctionVariant getAggregateFunction(int reference, SimpleExtension.ExtensionCollection extensions);
}
