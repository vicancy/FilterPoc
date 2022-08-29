using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.OData.Edm;
using Microsoft.OData.UriParser;

namespace FilterPoc
{
    internal class FilterBinder
    {
        public static Expression FromFilterClause(FilterClause clause)
        {

        }

        internal static Expression Bind(
           IQueryable baseQuery,
           FilterClause filterClause,
           Type filterType,
           ODataQueryContext context,
           ODataQuerySettings querySettings)
        {
            FilterBinder binder = new FilterBinder(querySettings, WebApiAssembliesResolver.Default, context.Model);

            binder._filterType = filterType;
            binder.BaseQuery = baseQuery;

            return BindFilterClause(binder, filterClause, filterType);
        }

        private static LambdaExpression BindFilterClause(FilterBinder binder, FilterClause filterClause, Type filterType)
        {
            LambdaExpression filter = binder.BindExpression(filterClause.Expression, filterClause.RangeVariable, filterType);
            filter = Expression.Lambda(binder.ApplyNullPropagationForFilterBody(filter.Body), filter.Parameters);

            Type expectedFilterType = typeof(Func<,>).MakeGenericType(filterType, typeof(bool));
            if (filter.Type != expectedFilterType)
            {
                throw new ArgumentException(filter.Type.Name);
            }

            return filter;
        }

        private const string ODataItParameterName = "$it";
        private const string ODataThisParameterName = "$this";

        private Stack<Dictionary<string, ParameterExpression>> _parametersStack = new Stack<Dictionary<string, ParameterExpression>>();
        private Dictionary<string, ParameterExpression> _lambdaParameters;
        private Type _filterType;

        private LambdaExpression BindExpression(SingleValueNode expression, RangeVariable rangeVariable, Type elementType)
        {
            ParameterExpression filterParameter = Expression.Parameter(elementType, rangeVariable.Name);
            _lambdaParameters = new Dictionary<string, ParameterExpression>();
            _lambdaParameters.Add(rangeVariable.Name, filterParameter);

            EnsureFlattenedPropertyContainer(filterParameter);

            Expression body = Bind(expression);
            return Expression.Lambda(body, filterParameter);
        }
        /// <summary>
        /// Binds a <see cref="QueryNode"/> to create a LINQ <see cref="Expression"/> that represents the semantics
        /// of the <see cref="QueryNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public override Expression Bind(QueryNode node)
        {
            // Recursion guard to avoid stack overflows
            RuntimeHelpers.EnsureSufficientExecutionStack();

            CollectionNode collectionNode = node as CollectionNode;
            SingleValueNode singleValueNode = node as SingleValueNode;

            if (collectionNode != null)
            {
                return BindCollectionNode(collectionNode);
            }
            else if (singleValueNode != null)
            {
                return BindSingleValueNode(singleValueNode);
            }
            else
            {
                throw Error.NotSupported("QueryNodeBindingNotSupported", node.Kind, typeof(FilterBinder).Name);
            }
        }

        /// <summary>
        /// Binds a <see cref="SingleValueNode"/> to create a LINQ <see cref="Expression"/> that represents the semantics
        /// of the <see cref="SingleValueNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        private Expression BindSingleValueNode(SingleValueNode node)
        {
            switch (node.Kind)
            {
                case QueryNodeKind.BinaryOperator:
                    return BindBinaryOperatorNode(node as BinaryOperatorNode);

                case QueryNodeKind.Constant:
                    return BindConstantNode(node as ConstantNode);

                case QueryNodeKind.Convert:
                    return BindConvertNode(node as ConvertNode);

                case QueryNodeKind.ResourceRangeVariableReference:
                    return BindRangeVariable((node as ResourceRangeVariableReferenceNode).RangeVariable);

                case QueryNodeKind.NonResourceRangeVariableReference:
                    return BindRangeVariable((node as NonResourceRangeVariableReferenceNode).RangeVariable);

                case QueryNodeKind.SingleValuePropertyAccess:
                    return BindPropertyAccessQueryNode(node as SingleValuePropertyAccessNode);

                case QueryNodeKind.SingleComplexNode:
                    return BindSingleComplexNode(node as SingleComplexNode);

                case QueryNodeKind.SingleValueOpenPropertyAccess:
                    return BindDynamicPropertyAccessQueryNode(node as SingleValueOpenPropertyAccessNode);

                case QueryNodeKind.UnaryOperator:
                    return BindUnaryOperatorNode(node as UnaryOperatorNode);

                case QueryNodeKind.SingleValueFunctionCall:
                    return BindSingleValueFunctionCallNode(node as SingleValueFunctionCallNode);

                case QueryNodeKind.SingleNavigationNode:
                    SingleNavigationNode navigationNode = node as SingleNavigationNode;
                    return BindNavigationPropertyNode(navigationNode.Source, navigationNode.NavigationProperty, GetFullPropertyPath(navigationNode));

                case QueryNodeKind.Any:
                    return BindAnyNode(node as AnyNode);

                case QueryNodeKind.All:
                    return BindAllNode(node as AllNode);

                case QueryNodeKind.SingleResourceCast:
                    return BindSingleResourceCastNode(node as SingleResourceCastNode);

                case QueryNodeKind.SingleResourceFunctionCall:
                    return BindSingleResourceFunctionCallNode(node as SingleResourceFunctionCallNode);

                case QueryNodeKind.In:
                    return BindInNode(node as InNode);

                case QueryNodeKind.Count:
                case QueryNodeKind.NamedFunctionParameter:
                case QueryNodeKind.ParameterAlias:
                case QueryNodeKind.EntitySet:
                case QueryNodeKind.KeyLookup:
                case QueryNodeKind.SearchTerm:
                // Unused or have unknown uses.
                default:
                    throw Error.NotSupported("QueryNodeBindingNotSupported", node.Kind, typeof(FilterBinder).Name);
            }
        }


        private Expression BindCountNode(CountNode node)
        {
            Expression source = Bind(node.Source);
            Expression countExpression = Expression.Constant(null, typeof(long?));
            Type elementType;
            if (!TypeHelper.IsCollection(source.Type, out elementType))
            {
                return countExpression;
            }

            MethodInfo countMethod;
            if (typeof(IQueryable).IsAssignableFrom(source.Type))
            {
                countMethod = ExpressionHelperMethods.QueryableCountGeneric.MakeGenericMethod(elementType);
            }
            else
            {
                countMethod = ExpressionHelperMethods.EnumerableCountGeneric.MakeGenericMethod(elementType);
            }

            MethodInfo whereMethod;
            if (typeof(IQueryable).IsAssignableFrom(source.Type))
            {
                whereMethod = ExpressionHelperMethods.QueryableWhereGeneric.MakeGenericMethod(elementType);
            }
            else
            {
                whereMethod = ExpressionHelperMethods.EnumerableWhereGeneric.MakeGenericMethod(elementType);
            }

            // Bind the inner $filter clause within the $count segment.
            // e.g Books?$filter=Authors/$count($filter=Id gt 1) gt 1
            Expression filterExpression = null;
            if (node.FilterClause != null)
            {
                filterExpression = BindFilterClause(this, node.FilterClause, elementType);

                // The source expression looks like: $it.Authors
                // So the generated source expression below will look like: $it.Authors.Where($it => $it.Id > 1)
                source = Expression.Call(null, whereMethod, new[] { source, filterExpression });
            }

            // append LongCount() method.
            // The final countExpression with the nested $filter clause will look like: $it.Authors.Where($it => $it.Id > 1).LongCount()
            // The final countExpression without the nested $filter clause will look like: $it.Authors.LongCount()
            countExpression = Expression.Call(null, countMethod, new[] { source });

            if (QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True)
            {
                // source == null ? null : countExpression 
                return Expression.Condition(
                       test: Expression.Equal(source, Expression.Constant(null)),
                       ifTrue: Expression.Constant(null, typeof(long?)),
                       ifFalse: ExpressionHelpers.ToNullable(countExpression));
            }
            else
            {
                return countExpression;
            }
        }

        /// <summary>
        /// Binds a <see cref="SingleResourceFunctionCallNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleResourceFunctionCallNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindSingleResourceFunctionCallNode(SingleResourceFunctionCallNode node)
        {
            switch (node.Name)
            {
                case ClrCanonicalFunctions.CastFunctionName:
                    return BindSingleResourceCastFunctionCall(node);
                default:
                    throw Error.NotSupported(SRResources.ODataFunctionNotSupported, node.Name);
            }
        }

        /// <summary>
        /// Binds a <see cref="SingleResourceCastNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleResourceCastNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindSingleResourceCastNode(SingleResourceCastNode node)
        {
            IEdmStructuredTypeReference structured = node.StructuredTypeReference;
            Contract.Assert(structured != null, "NS casts can contain only structured types");

            Type clrType = EdmLibHelpers.GetClrType(structured, Model);

            Expression source = BindCastSourceNode(node.Source);
            return Expression.TypeAs(source, clrType);
        }


        /// <summary>
        /// Binds a <see cref="AnyNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="AnyNode"/>.
        /// </summary>
        /// <param name="anyNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindAnyNode(AnyNode anyNode)
        {
            ParameterExpression anyIt = HandleLambdaParameters(anyNode.RangeVariables);

            Expression source;
            Contract.Assert(anyNode.Source != null);
            source = Bind(anyNode.Source);

            Expression body = null;
            // uri parser places an Constant node with value true for empty any() body
            if (anyNode.Body != null && anyNode.Body.Kind != QueryNodeKind.Constant)
            {
                body = Bind(anyNode.Body);
                body = ApplyNullPropagationForFilterBody(body);
                body = Expression.Lambda(body, anyIt);
            }
            else if (anyNode.Body != null && anyNode.Body.Kind == QueryNodeKind.Constant
                && (bool)(anyNode.Body as ConstantNode).Value == false)
            {
                // any(false) is the same as just false
                ExitLamdbaScope();
                return FalseConstant;
            }

            Expression any = Any(source, body);

            ExitLamdbaScope();

            if (QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True && IsNullable(source.Type))
            {
                // IFF(source == null) null; else Any(body);
                any = ToNullable(any);
                return Expression.Condition(
                    test: Expression.Equal(source, NullConstant),
                    ifTrue: Expression.Constant(null, any.Type),
                    ifFalse: any);
            }
            else
            {
                return any;
            }
        }



        /// <summary>
        /// Binds a <see cref="AllNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="AllNode"/>.
        /// </summary>
        /// <param name="allNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindAllNode(AllNode allNode)
        {
            ParameterExpression allIt = HandleLambdaParameters(allNode.RangeVariables);

            Expression source;
            Contract.Assert(allNode.Source != null);
            source = Bind(allNode.Source);

            Expression body = source;
            Contract.Assert(allNode.Body != null);

            body = Bind(allNode.Body);
            body = ApplyNullPropagationForFilterBody(body);
            body = Expression.Lambda(body, allIt);

            Expression all = All(source, body);

            ExitLamdbaScope();

            if (QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True && IsNullable(source.Type))
            {
                // IFF(source == null) null; else Any(body);
                all = ToNullable(all);
                return Expression.Condition(
                    test: Expression.Equal(source, NullConstant),
                    ifTrue: Expression.Constant(null, all.Type),
                    ifFalse: all);
            }
            else
            {
                return all;
            }
        }
        /// <summary>
        /// Binds a <see cref="IEdmNavigationProperty"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="IEdmNavigationProperty"/>.
        /// </summary>
        /// <param name="sourceNode">The node that represents the navigation source.</param>
        /// <param name="navigationProperty">The navigation property to bind.</param>
        /// <param name="propertyPath">The property path.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindNavigationPropertyNode(QueryNode sourceNode, IEdmNavigationProperty navigationProperty, string propertyPath)
        {
            Expression source;

            // TODO: bug in uri parser is causing this property to be null for the root property.
            if (sourceNode == null)
            {
                source = _lambdaParameters[ODataItParameterName];
            }
            else
            {
                source = Bind(sourceNode);
            }

            return CreatePropertyAccessExpression(source, navigationProperty, propertyPath);
        }

        /// <summary>
        /// Binds a <see cref="SingleValueFunctionCallNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleValueFunctionCallNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        [SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity",
                        Justification = "These are simple binding functions and cannot be split up.")]
        public virtual Expression BindSingleValueFunctionCallNode(SingleValueFunctionCallNode node)
        {
            switch (node.Name)
            {
                case ClrCanonicalFunctions.StartswithFunctionName:
                    return BindStartsWith(node);

                case ClrCanonicalFunctions.EndswithFunctionName:
                    return BindEndsWith(node);

                case ClrCanonicalFunctions.ContainsFunctionName:
                    return BindContains(node);

                case ClrCanonicalFunctions.SubstringFunctionName:
                    return BindSubstring(node);

                case ClrCanonicalFunctions.LengthFunctionName:
                    return BindLength(node);

                case ClrCanonicalFunctions.IndexofFunctionName:
                    return BindIndexOf(node);

                case ClrCanonicalFunctions.TolowerFunctionName:
                    return BindToLower(node);

                case ClrCanonicalFunctions.ToupperFunctionName:
                    return BindToUpper(node);

                case ClrCanonicalFunctions.TrimFunctionName:
                    return BindTrim(node);

                case ClrCanonicalFunctions.ConcatFunctionName:
                    return BindConcat(node);

                case ClrCanonicalFunctions.YearFunctionName:
                case ClrCanonicalFunctions.MonthFunctionName:
                case ClrCanonicalFunctions.DayFunctionName:
                    return BindDateRelatedProperty(node); // Date & DateTime & DateTimeOffset

                case ClrCanonicalFunctions.HourFunctionName:
                case ClrCanonicalFunctions.MinuteFunctionName:
                case ClrCanonicalFunctions.SecondFunctionName:
                    return BindTimeRelatedProperty(node); // TimeOfDay & DateTime & DateTimeOffset

                case ClrCanonicalFunctions.FractionalSecondsFunctionName:
                    return BindFractionalSeconds(node);

                case ClrCanonicalFunctions.RoundFunctionName:
                    return BindRound(node);

                case ClrCanonicalFunctions.FloorFunctionName:
                    return BindFloor(node);

                case ClrCanonicalFunctions.CeilingFunctionName:
                    return BindCeiling(node);

                case ClrCanonicalFunctions.CastFunctionName:
                    return BindCastSingleValue(node);

                case ClrCanonicalFunctions.IsofFunctionName:
                    return BindIsOf(node);

                case ClrCanonicalFunctions.DateFunctionName:
                    return BindDate(node);

                case ClrCanonicalFunctions.TimeFunctionName:
                    return BindTime(node);

                case ClrCanonicalFunctions.NowFunctionName:
                    return BindNow(node);

                default:
                    // Get Expression of custom binded method.
                    Expression expression = BindCustomMethodExpressionOrNull(node);
                    if (expression != null)
                    {
                        return expression;
                    }

                    throw new NotImplementedException(Error.Format(SRResources.ODataFunctionNotSupported, node.Name));
            }
        }
        /// <summary>
        /// Binds a <see cref="SingleValueOpenPropertyAccessNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleValueOpenPropertyAccessNode"/>.
        /// </summary>
        /// <param name="openNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindDynamicPropertyAccessQueryNode(SingleValueOpenPropertyAccessNode openNode)
        {
            if (EdmLibHelpers.IsDynamicTypeWrapper(_filterType))
            {
                return GetFlattenedPropertyExpression(openNode.Name) ?? Expression.Property(Bind(openNode.Source), openNode.Name);
            }
            PropertyInfo prop = GetDynamicPropertyContainer(openNode);

            var propertyAccessExpression = BindPropertyAccessExpression(openNode, prop);
            var readDictionaryIndexerExpression = Expression.Property(propertyAccessExpression,
                DictionaryStringObjectIndexerName, Expression.Constant(openNode.Name));
            var containsKeyExpression = Expression.Call(propertyAccessExpression,
                propertyAccessExpression.Type.GetMethod("ContainsKey"), Expression.Constant(openNode.Name));
            var nullExpression = Expression.Constant(null);

            if (QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True)
            {
                var dynamicDictIsNotNull = Expression.NotEqual(propertyAccessExpression, Expression.Constant(null));
                var dynamicDictIsNotNullAndContainsKey = Expression.AndAlso(dynamicDictIsNotNull, containsKeyExpression);
                return Expression.Condition(
                    dynamicDictIsNotNullAndContainsKey,
                    readDictionaryIndexerExpression,
                    nullExpression);
            }
            else
            {
                return Expression.Condition(
                    containsKeyExpression,
                    readDictionaryIndexerExpression,
                    nullExpression);
            }
        }
        /// <summary>
        /// Binds a <see cref="CollectionPropertyAccessNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="CollectionPropertyAccessNode"/>.
        /// </summary>
        /// <param name="propertyAccessNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindCollectionPropertyAccessNode(CollectionPropertyAccessNode propertyAccessNode)
        {
            Expression source = Bind(propertyAccessNode.Source);
            return CreatePropertyAccessExpression(source, propertyAccessNode.Property);
        }

        /// <summary>
        /// Binds a <see cref="CollectionComplexNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="CollectionComplexNode"/>.
        /// </summary>
        /// <param name="collectionComplexNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindCollectionComplexNode(CollectionComplexNode collectionComplexNode)
        {
            Expression source = Bind(collectionComplexNode.Source);
            return CreatePropertyAccessExpression(source, collectionComplexNode.Property);
        }

        /// <summary>
        /// Binds a <see cref="SingleValuePropertyAccessNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleValuePropertyAccessNode"/>.
        /// </summary>
        /// <param name="propertyAccessNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindPropertyAccessQueryNode(SingleValuePropertyAccessNode propertyAccessNode)
        {
            Expression source = Bind(propertyAccessNode.Source);
            return CreatePropertyAccessExpression(source, propertyAccessNode.Property, GetFullPropertyPath(propertyAccessNode));
        }

        /// <summary>
        /// Binds a <see cref="SingleComplexNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="SingleComplexNode"/>.
        /// </summary>
        /// <param name="singleComplexNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindSingleComplexNode(SingleComplexNode singleComplexNode)
        {
            Expression source = Bind(singleComplexNode.Source);
            return CreatePropertyAccessExpression(source, singleComplexNode.Property, GetFullPropertyPath(singleComplexNode));
        }

        /// <summary>
        /// Binds a <see cref="UnaryOperatorNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="UnaryOperatorNode"/>.
        /// </summary>
        /// <param name="unaryOperatorNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindUnaryOperatorNode(UnaryOperatorNode unaryOperatorNode)
        {
            // No need to handle null-propagation here as CLR already handles it.
            // !(null) = null
            // -(null) = null
            Expression inner = Bind(unaryOperatorNode.Operand);
            switch (unaryOperatorNode.OperatorKind)
            {
                case UnaryOperatorKind.Negate:
                    return Expression.Negate(inner);

                case UnaryOperatorKind.Not:
                    return Expression.Not(inner);

                default:
                    throw Error.NotSupported(SRResources.QueryNodeBindingNotSupported, unaryOperatorNode.Kind, typeof(FilterBinder).Name);
            }
        }

        /// <summary>
        /// Binds a <see cref="RangeVariable"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="RangeVariable"/>.
        /// </summary>
        /// <param name="rangeVariable">The range variable to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindRangeVariable(RangeVariable rangeVariable)
        {
            ParameterExpression parameter = null;

            // When we have a $this RangeVariable, we still create a $it parameter.
            // i.e $it => $it instead of $this => $this
            if (rangeVariable.Name == ODataThisParameterName)
            {
                parameter = _lambdaParameters[ODataItParameterName];
            }
            else
            {
                parameter = _lambdaParameters[rangeVariable.Name];
            }
            return ConvertNonStandardPrimitives(parameter);
        }

        /// <summary>
        /// Binds a <see cref="ConstantNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="ConstantNode"/>.
        /// </summary>
        /// <param name="constantNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindConstantNode(ConstantNode constantNode)
        {
            Contract.Assert(constantNode != null);

            // no need to parameterize null's as there cannot be multiple values for null.
            if (constantNode.Value == null)
            {
                return NullConstant;
            }

            object value = constantNode.Value;
            Type constantType = RetrieveClrTypeForConstant(constantNode.TypeReference, ref value);

            if (QuerySettings.EnableConstantParameterization)
            {
                return LinqParameterContainer.Parameterize(constantType, value);
            }
            else
            {
                return Expression.Constant(value, constantType);
            }
        }
        /// <summary>
        /// Binds a <see cref="BinaryOperatorNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="BinaryOperatorNode"/>.
        /// </summary>
        /// <param name="binaryOperatorNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindBinaryOperatorNode(BinaryOperatorNode binaryOperatorNode)
        {
            Expression left = Bind(binaryOperatorNode.Left);
            Expression right = Bind(binaryOperatorNode.Right);

            bool containsDateFunction = ContainsDateFunction(binaryOperatorNode);
            // handle null propagation only if either of the operands can be null
            bool isNullPropagationRequired = QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True && (IsNullable(left.Type) || IsNullable(right.Type));
            if (isNullPropagationRequired)
            {
                // |----------------------------------------------------------------|
                // |SQL 3VL truth table.                                            |
                // |----------------------------------------------------------------|
                // |p       |    q      |    p OR q     |    p AND q    |    p = q  |
                // |----------------------------------------------------------------|
                // |True    |   True    |   True        |   True        |   True    |
                // |True    |   False   |   True        |   False       |   False   |
                // |True    |   NULL    |   True        |   NULL        |   NULL    |
                // |False   |   True    |   True        |   False       |   False   |
                // |False   |   False   |   False       |   False       |   True    |
                // |False   |   NULL    |   NULL        |   False       |   NULL    |
                // |NULL    |   True    |   True        |   NULL        |   NULL    |
                // |NULL    |   False   |   NULL        |   False       |   NULL    |
                // |NULL    |   NULL    |   Null        |   NULL        |   NULL    |
                // |--------|-----------|---------------|---------------|-----------|

                // before we start with null propagation, convert the operators to nullable if already not.
                left = ToNullable(left);
                right = ToNullable(right);

                bool liftToNull = true;
                if (left == NullConstant || right == NullConstant)
                {
                    liftToNull = false;
                }

                // Expression trees do a very good job of handling the 3VL truth table if we pass liftToNull true.
                return CreateBinaryExpression(binaryOperatorNode.OperatorKind, left, right, liftToNull: liftToNull, containsDateFunction);
            }
            else
            {
                return CreateBinaryExpression(binaryOperatorNode.OperatorKind, left, right, liftToNull: false, containsDateFunction);
            }
        }

        /// <summary>
        /// Binds an <see cref="InNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="InNode"/>.
        /// </summary>
        /// <param name="inNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindInNode(InNode inNode)
        {
            Expression singleValue = Bind(inNode.Left);
            Expression collection = Bind(inNode.Right);

            Type collectionItemType = collection.Type.GetElementType();
            if (collectionItemType == null)
            {
                Type[] genericArgs = collection.Type.GetGenericArguments();
                // The model builder does not support non-generic collections like ArrayList
                // or generic collections with generic arguments > 1 like IDictionary<,>
                Contract.Assert(genericArgs.Length == 1);
                collectionItemType = genericArgs[0];
            }

            if (IsIQueryable(collection.Type))
            {
                Expression containsExpression = singleValue.Type != collectionItemType ? Expression.Call(null, ExpressionHelperMethods.QueryableCastGeneric.MakeGenericMethod(singleValue.Type), collection) : collection;
                return Expression.Call(null, ExpressionHelperMethods.QueryableContainsGeneric.MakeGenericMethod(singleValue.Type), containsExpression, singleValue);
            }
            else
            {
                Expression containsExpression = singleValue.Type != collectionItemType ? Expression.Call(null, ExpressionHelperMethods.EnumerableCastGeneric.MakeGenericMethod(singleValue.Type), collection) : collection;
                return Expression.Call(null, ExpressionHelperMethods.EnumerableContainsGeneric.MakeGenericMethod(singleValue.Type), containsExpression, singleValue);
            }
        }

        /// <summary>
        /// Binds a <see cref="ConvertNode"/> to create a LINQ <see cref="Expression"/> that
        /// represents the semantics of the <see cref="ConvertNode"/>.
        /// </summary>
        /// <param name="convertNode">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        public virtual Expression BindConvertNode(ConvertNode convertNode)
        {
            Contract.Assert(convertNode != null);
            Contract.Assert(convertNode.TypeReference != null);

            Expression source = Bind(convertNode.Source);

            return CreateConvertExpression(convertNode, source);
        }
        /// <summary>
        /// Binds a <see cref="CollectionNode"/> to create a LINQ <see cref="Expression"/> that represents the semantics
        /// of the <see cref="CollectionNode"/>.
        /// </summary>
        /// <param name="node">The node to bind.</param>
        /// <returns>The LINQ <see cref="Expression"/> created.</returns>
        private Expression BindCollectionNode(CollectionNode node)
        {
            switch (node.Kind)
            {
                case QueryNodeKind.CollectionNavigationNode:
                    CollectionNavigationNode navigationNode = node as CollectionNavigationNode;
                    return BindNavigationPropertyNode(navigationNode.Source, navigationNode.NavigationProperty);

                case QueryNodeKind.CollectionPropertyAccess:
                    return BindCollectionPropertyAccessNode(node as CollectionPropertyAccessNode);

                case QueryNodeKind.CollectionComplexNode:
                    return BindCollectionComplexNode(node as CollectionComplexNode);

                case QueryNodeKind.CollectionResourceCast:
                    return BindCollectionResourceCastNode(node as CollectionResourceCastNode);

                case QueryNodeKind.CollectionConstant:
                    return BindCollectionConstantNode(node as CollectionConstantNode);

                case QueryNodeKind.CollectionFunctionCall:
                case QueryNodeKind.CollectionResourceFunctionCall:
                case QueryNodeKind.CollectionOpenPropertyAccess:
                default:
                    throw Error.NotSupported(SRResources.QueryNodeBindingNotSupported, node.Kind, typeof(FilterBinder).Name);
            }
        }

        internal static bool IsNullable(Type t)
        {
            if (!TypeHelper.IsValueType(t) || (TypeHelper.IsGenericType(t) && t.GetGenericTypeDefinition() == typeof(Nullable<>)))
            {
                return true;
            }

            return false;
        }
        private Expression ApplyNullPropagationForFilterBody(Expression body)
        {
            if (IsNullable(body.Type))
            {
                body = Expression.Convert(body, typeof(bool));
            }

            return body;
        }

        internal static readonly MethodInfo StringCompareMethodInfo = typeof(string).GetMethod("Compare", new[] { typeof(string), typeof(string) });
        internal static readonly MethodInfo GuidCompareMethodInfo = typeof(Guid).GetMethod("CompareTo", new[] { typeof(Guid) });
        internal static readonly string DictionaryStringObjectIndexerName = typeof(Dictionary<string, object>).GetDefaultMembers()[0].Name;

        internal static readonly Expression NullConstant = Expression.Constant(null);
        internal static readonly Expression FalseConstant = Expression.Constant(false);
        internal static readonly Expression TrueConstant = Expression.Constant(true);
        internal static readonly Expression ZeroConstant = Expression.Constant(0);

        internal static readonly MethodInfo EnumTryParseMethod = typeof(Enum).GetMethods()
            .Single(m => m.Name.Equals("TryParse", StringComparison.Ordinal)
                && m.GetParameters().Length == 2
                && m.GetParameters()[0].ParameterType.Equals(typeof(string)));

        internal static readonly Dictionary<BinaryOperatorKind, ExpressionType> BinaryOperatorMapping = new Dictionary<BinaryOperatorKind, ExpressionType>
        {
            { BinaryOperatorKind.Add, ExpressionType.Add },
            { BinaryOperatorKind.And, ExpressionType.AndAlso },
            { BinaryOperatorKind.Divide, ExpressionType.Divide },
            { BinaryOperatorKind.Equal, ExpressionType.Equal },
            { BinaryOperatorKind.GreaterThan, ExpressionType.GreaterThan },
            { BinaryOperatorKind.GreaterThanOrEqual, ExpressionType.GreaterThanOrEqual },
            { BinaryOperatorKind.LessThan, ExpressionType.LessThan },
            { BinaryOperatorKind.LessThanOrEqual, ExpressionType.LessThanOrEqual },
            { BinaryOperatorKind.Modulo, ExpressionType.Modulo },
            { BinaryOperatorKind.Multiply, ExpressionType.Multiply },
            { BinaryOperatorKind.NotEqual, ExpressionType.NotEqual },
            { BinaryOperatorKind.Or, ExpressionType.OrElse },
            { BinaryOperatorKind.Subtract, ExpressionType.Subtract },
        };

        /// <summary>
        /// Base query used for the binder.
        /// </summary>
        internal IQueryable BaseQuery;

        /// <summary>
        /// Flattened list of properties from base query, for case when binder is applied for aggregated query.
        /// </summary>
        internal IDictionary<string, Expression> FlattenedPropertyContainer;

        internal bool HasInstancePropertyContainer;

        /// <summary>
        /// Analyze previous query and extract grouped properties.
        /// </summary>
        /// <param name="source"></param>
        protected void EnsureFlattenedPropertyContainer(ParameterExpression source)
        {
            if (this.BaseQuery != null)
            {
                this.HasInstancePropertyContainer = this.BaseQuery.ElementType.IsGenericType
                    && this.BaseQuery.ElementType.GetGenericTypeDefinition() == typeof(ComputeWrapper<>);

                this.FlattenedPropertyContainer = this.FlattenedPropertyContainer ?? this.GetFlattenedProperties(source);
            }
        }

        internal IDictionary<string, Expression> GetFlattenedProperties(ParameterExpression source)
        {
            if (this.BaseQuery == null)
            {
                return null;
            }

            //if (!typeof(GroupByWrapper).IsAssignableFrom(BaseQuery.ElementType))
            //{
            //    return null;
            //}

            var expression = BaseQuery.Expression as MethodCallExpression;
            if (expression == null)
            {
                return null;
            }

            // After $apply we could have other clauses, like $filter, $orderby etc.
            // Skip of filter expressions
            expression = SkipFilters(expression);

            if (expression == null)
            {
                return null;
            }

            var result = new Dictionary<string, Expression>();
            CollectContainerAssignments(source, expression, result);
            if (this.HasInstancePropertyContainer)
            {
                var instanceProperty = Expression.Property(source, "Instance");
                if (typeof(DynamicTypeWrapper).IsAssignableFrom(instanceProperty.Type))
                {
                    var computeExpression = expression.Arguments.FirstOrDefault() as MethodCallExpression;
                    computeExpression = SkipFilters(computeExpression);
                    if (computeExpression != null)
                    {
                        CollectContainerAssignments(instanceProperty, computeExpression, result);
                    }
                }
            }

            return result;
        }

        internal class ComputeWrapper<T> : GroupByWrapper, IEdmEntityObject
        {
            public T Instance { get; set; }

            /// <summary>
            /// An ID to uniquely identify the model in the <see cref="ModelContainer"/>.
            /// </summary>
            public string ModelID { get; set; }

            public override Dictionary<string, object> Values
            {
                get
                {
                    EnsureValues();
                    return base.Values;
                }
            }

            private bool _merged;
            private void EnsureValues()
            {
                if (!this._merged)
                {
                    // Base properties available via Instance can be real OData properties or generated in previous transformations

                    var instanceContainer = this.Instance as DynamicTypeWrapper;
                    if (instanceContainer != null)
                    {
                        // Add proeprties generated in previous transformations to the collection
                        base.Values.MergeWithReplace(instanceContainer.Values);
                    }
                    else
                    {
                        // Add real OData properties to the collection
                        // We need to use injected Model to real property names
                        var edmType = GetEdmType() as IEdmStructuredTypeReference;

                        if (edmType is IEdmComplexTypeReference t)
                        {
                            _typedEdmStructuredObject = _typedEdmStructuredObject ??
                            new TypedEdmComplexObject(Instance, t, GetModel());
                        }
                        else
                        {
                            _typedEdmStructuredObject = _typedEdmStructuredObject ??
                            new TypedEdmEntityObject(Instance, edmType as IEdmEntityTypeReference, GetModel());
                        }

                        var props = edmType.DeclaredStructuralProperties().Where(p => p.Type.IsPrimitive()).Select(p => p.Name);
                        foreach (var propertyName in props)
                        {
                            object value;
                            if (_typedEdmStructuredObject.TryGetPropertyValue(propertyName, out value))
                            {
                                base.Values[propertyName] = value;
                            }
                        }
                    }
                    this._merged = true;
                }
            }
            private TypedEdmStructuredObject _typedEdmStructuredObject;

            private IEdmModel GetModel()
            {
                Contract.Assert(ModelID != null);

                return ModelContainer.GetModel(ModelID);
            }

            public IEdmTypeReference GetEdmType()
            {
                IEdmModel model = GetModel();
                return model.GetEdmTypeReference(typeof(T));
            }
        }
        /// <summary>
        /// Represents a container class that contains properties that are grouped by using $apply.
        /// </summary>
        public abstract class DynamicTypeWrapper
    {
        /// <summary>
        /// Gets values stored in the wrapper
        /// </summary>
        public abstract Dictionary<string, object> Values { get; }

        /// <summary>
        /// Attempts to get the value of the Property called <paramref name="propertyName"/> from the underlying Entity.
        /// </summary>
        /// <param name="propertyName">The name of the Property</param>
        /// <param name="value">The new value of the Property</param>
        /// <returns>True if successful</returns>
        public bool TryGetPropertyValue(string propertyName, out object value)
        {
            return this.Values.TryGetValue(propertyName, out value);
        }
    }

        private static void CollectContainerAssignments(Expression source, MethodCallExpression expression, Dictionary<string, Expression> result)
        {
            CollectAssigments(result, Expression.Property(source, "GroupByContainer"), ExtractContainerExpression(expression.Arguments.FirstOrDefault() as MethodCallExpression, "GroupByContainer"));
            CollectAssigments(result, Expression.Property(source, "Container"), ExtractContainerExpression(expression, "Container"));
        }


        private static MemberInitExpression ExtractContainerExpression(MethodCallExpression expression, string containerName)
        {
            if (expression == null || expression.Arguments.Count < 2)
            {
                return null;
            }

            var memberInitExpression = ((expression.Arguments[1] as UnaryExpression).Operand as LambdaExpression).Body as MemberInitExpression;
            if (memberInitExpression != null)
            {
                var containerAssigment = memberInitExpression.Bindings.FirstOrDefault(m => m.Member.Name == containerName) as MemberAssignment;
                if (containerAssigment != null)
                {
                    return containerAssigment.Expression as MemberInitExpression;
                }
            }
            return null;
        }

        private static void CollectAssigments(IDictionary<string, Expression> flattenPropertyContainer, Expression source, MemberInitExpression expression, string prefix = null)
        {
            if (expression == null)
            {
                return;
            }

            string nameToAdd = null;
            Type resultType = null;
            MemberInitExpression nextExpression = null;
            Expression nestedExpression = null;
            foreach (var expr in expression.Bindings.OfType<MemberAssignment>())
            {
                var initExpr = expr.Expression as MemberInitExpression;
                if (initExpr != null && expr.Member.Name == "Next")
                {
                    nextExpression = initExpr;
                }
                else if (expr.Member.Name == "Name")
                {
                    nameToAdd = (expr.Expression as ConstantExpression).Value as string;
                }
                else if (expr.Member.Name == "Value" || expr.Member.Name == "NestedValue")
                {
                    resultType = expr.Expression.Type;
                    if (resultType == typeof(object) && expr.Expression.NodeType == ExpressionType.Convert)
                    {
                        resultType = ((UnaryExpression)expr.Expression).Operand.Type;
                    }

                    //if (typeof(GroupByWrapper).IsAssignableFrom(resultType))
                    //{
                    //    nestedExpression = expr.Expression;
                    //}
                }
            }

            if (prefix != null)
            {
                nameToAdd = prefix + "\\" + nameToAdd;
            }

            //if (typeof(GroupByWrapper).IsAssignableFrom(resultType))
            //{
            //    flattenPropertyContainer.Add(nameToAdd, Expression.Property(source, "NestedValue"));
            //}
            //else
            {
                flattenPropertyContainer.Add(nameToAdd, Expression.Convert(Expression.Property(source, "Value"), resultType));
            }

            if (nextExpression != null)
            {
                CollectAssigments(flattenPropertyContainer, Expression.Property(source, "Next"), nextExpression, prefix);
            }

            if (nestedExpression != null)
            {
                var nestedAccessor = ((nestedExpression as MemberInitExpression).Bindings.First() as MemberAssignment).Expression as MemberInitExpression;
                var newSource = Expression.Property(Expression.Property(source, "NestedValue"), "GroupByContainer");
                CollectAssigments(flattenPropertyContainer, newSource, nestedAccessor, nameToAdd);
            }
        }
        private static MethodCallExpression SkipFilters(MethodCallExpression expression)
        {
            while (expression.Method.Name == "Where")
            {
                expression = expression.Arguments.FirstOrDefault() as MethodCallExpression;
            }

            return expression;
        }
    }
}
