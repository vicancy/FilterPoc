using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Microsoft.OData.Edm;
using Microsoft.OData.UriParser;
using Microsoft.OData.UriParser.Aggregation;

namespace FilterPoc.Shared
{
    internal class NodeToExpression : QueryNodeVisitor<Expression>
    {
        /// <summary>
        /// Expression to represent the implicit variable parameter in filter 
        /// </summary>
        public Expression ImplicitVariableParameterExpression;

        public override Expression Visit(AllNode nodeIn)
        {
            var instanceType = EdmClrTypeUtils.GetInstanceType(nodeIn.RangeVariables[0].TypeReference);
            ParameterExpression parameter = Expression.Parameter(instanceType, nodeIn.RangeVariables[0].Name);
            NodeToExpression nodeToExpressionTranslator = new NodeToExpression()
            {
                ImplicitVariableParameterExpression = parameter,
            };
            Expression conditionExpression = nodeToExpressionTranslator.TranslateNode(nodeIn.Body);
            Expression rootExpression = this.TranslateNode(nodeIn.Source);

            return Expression.Call(
                typeof(Enumerable),
                "All",
                new Type[] { instanceType },
                rootExpression,
                Expression.Lambda(conditionExpression, parameter));
        }

        public override Expression Visit(AnyNode nodeIn)
        {
            var instanceType = EdmClrTypeUtils.GetInstanceType(nodeIn.RangeVariables[0].TypeReference);
            ParameterExpression parameter = Expression.Parameter(instanceType, nodeIn.RangeVariables[0].Name);
            NodeToExpression nodeToExpressionTranslator = new NodeToExpression()
            {
                ImplicitVariableParameterExpression = parameter
            };
            Expression conditionExpression = nodeToExpressionTranslator.TranslateNode(nodeIn.Body);
            Expression rootExpression = TranslateNode(nodeIn.Source);

            return Expression.Call(
                typeof(Enumerable),
                "Any",
                new Type[] { instanceType },
                rootExpression,
                Expression.Lambda(conditionExpression, parameter));
        }

        public override Expression Visit(BinaryOperatorNode nodeIn)
        {
            var left = TranslateNode(nodeIn.Left);
            var right = TranslateNode(nodeIn.Right);
            switch (nodeIn.OperatorKind)
            {
                case BinaryOperatorKind.Equal:
                case BinaryOperatorKind.NotEqual:
                case BinaryOperatorKind.GreaterThan:
                case BinaryOperatorKind.GreaterThanOrEqual:
                case BinaryOperatorKind.LessThan:
                case BinaryOperatorKind.LessThanOrEqual:
                case BinaryOperatorKind.Has:
                    return TranslateComparison(nodeIn.OperatorKind, left, right);
                case BinaryOperatorKind.Or:
                case BinaryOperatorKind.And:
                    {
                        if (Nullable.GetUnderlyingType(left.Type) != null)
                        {
                            left = Expression.Call(left, "GetValueOrDefault", Type.EmptyTypes);
                        }
                        if (Nullable.GetUnderlyingType(right.Type) != null)
                        {
                            right = Expression.Call(right, "GetValueOrDefault", Type.EmptyTypes);
                        }

                        return nodeIn.OperatorKind == BinaryOperatorKind.And
                            ? Expression.And(left, right)
                            : Expression.Or(left, right);
                    }
                case BinaryOperatorKind.Add:
                case BinaryOperatorKind.Subtract:
                case BinaryOperatorKind.Multiply:
                case BinaryOperatorKind.Divide:
                case BinaryOperatorKind.Modulo:
                default:
                    throw new NotSupportedException(nodeIn.OperatorKind.ToString());
            }
            return base.Visit(nodeIn);
        }

        public override Expression Visit(CountNode nodeIn)
        {
            throw new NotSupportedException();
        }

        public override Expression Visit(CollectionNavigationNode nodeIn)
        {
            return Expression.Property(this.ImplicitVariableParameterExpression,
                nodeIn.NavigationProperty.Name);
        }

        public override Expression Visit(CollectionPropertyAccessNode nodeIn)
        {
            return TranslatePropertyAccess(nodeIn.Source, nodeIn.Property);
        }

        public override Expression Visit(CollectionOpenPropertyAccessNode nodeIn)
        {
            throw new NotSupportedException();
        }

        internal static readonly Expression NullConstant = Expression.Constant(null);
        internal static readonly Expression FalseConstant = Expression.Constant(false);
        internal static readonly Expression TrueConstant = Expression.Constant(true);

        private Type RetrieveClrTypeForConstant(IEdmTypeReference edmTypeReference)
        {
            if (!edmTypeReference.IsPrimitive())
            {
                throw new NotSupportedException("Only support primitive constants");
            }

            return EdmClrTypeUtils.GetPrimitiveClrType(edmTypeReference.AsPrimitive().PrimitiveKind(), true);
        }

        /// <summary>
        /// Only supporting primitive types
        /// </summary>
        /// <param name="nodeIn"></param>
        /// <returns></returns>
        public override Expression Visit(ConstantNode nodeIn)
        {
            // no need to parameterize null's as there cannot be multiple values for null.
            if (nodeIn.Value == null)
            {
                return NullConstant;
            }

            Type constantType = RetrieveClrTypeForConstant(nodeIn.TypeReference);
            return Expression.Constant(nodeIn.Value, constantType);
        }

        public override Expression Visit(CollectionConstantNode nodeIn)
        {
            // It's fine if the collection is empty; the returned value will be an empty list.
            ConstantNode firstNode = nodeIn.Collection.FirstOrDefault();
            object value = null;
            if (firstNode != null)
            {
                value = firstNode.Value;
            }

            Type constantType = RetrieveClrTypeForConstant(nodeIn.ItemType);
            Type nullableConstantType = nodeIn.ItemType.IsNullable && constantType.IsValueType && Nullable.GetUnderlyingType(constantType) == null
                ? typeof(Nullable<>).MakeGenericType(constantType)
                : constantType;
            Type listType = typeof(List<>).MakeGenericType(nullableConstantType);
            IList castedList = Activator.CreateInstance(listType) as IList;

            // Getting a LINQ expression to dynamically cast each item in the Collection during runtime is tricky,
            // so using a foreach loop and doing an implicit cast from object to the CLR type of ItemType.
            foreach (ConstantNode item in nodeIn.Collection)
            {
                object member;
                if (item.Value == null)
                {
                    member = null;
                }
                else if (constantType.IsEnum)
                {
                    throw new NotSupportedException("Enum is not supported");
                }
                else
                {
                    member = item.Value;
                }

                castedList.Add(member);
            }

            return Expression.Constant(castedList, listType);
        }

        public override Expression Visit(ConvertNode nodeIn)
        {
            var sourceExpression = TranslateNode(nodeIn.Source);

            var targetEdmType = nodeIn.TypeReference;
            if (null == targetEdmType)
            {
                //Open property's target type is null, so return the source expression directly, supposely the caller should be ready to handle data of Object type.
                return sourceExpression;
            }
            var targetClrType = EdmClrTypeUtils.GetInstanceType(targetEdmType);
            return Expression.Convert(sourceExpression, targetClrType);
        }

        public override Expression Visit(CollectionResourceCastNode nodeIn)
        {
            throw new NotSupportedException("CollectionResourceCastNode");
        }

        public override Expression Visit(ResourceRangeVariableReferenceNode nodeIn)
        {
            // when this is called for a filter like svc/Customers?$filter=PersonID eq 1, nodeIn.Name has value "$it".
            // when this is called by any/all option, nodeIn.Name is specified by client, it can be any value.
            return ImplicitVariableParameterExpression;
        }

        public override Expression Visit(NonResourceRangeVariableReferenceNode nodeIn)
        {
            return ImplicitVariableParameterExpression;
        }

        public override Expression Visit(SingleResourceCastNode nodeIn)
        {
            return TranslateSingleValueCastAccess(nodeIn.Source, nodeIn.TypeReference);
        }

        private Expression TranslateSingleValueCastAccess(QueryNode sourceNode, IEdmTypeReference typeReference)
        {
            Expression source = TranslateNode(sourceNode);
            Type targetType = EdmClrTypeUtils.GetInstanceType(typeReference);
            return Expression.TypeAs(source, targetType);
        }
        private Expression TranslateSingleNavigationAccess(QueryNode sourceNode, IEdmProperty edmProperty)
        {
            Expression source = TranslateNode(sourceNode);
            return Expression.Property(source, edmProperty.Name);
        }

        public override Expression Visit(SingleNavigationNode nodeIn)
        {
            return TranslateSingleNavigationAccess(nodeIn.Source, nodeIn.NavigationProperty);
        }

        public override Expression Visit(SingleResourceFunctionCallNode nodeIn)
        {
            throw new NotSupportedException("SingleResourceFunctionCallNode");
        }

        public override Expression Visit(SingleValueFunctionCallNode nodeIn)
        {
            return TranslateFunctionCall(nodeIn.Name, nodeIn.Parameters);
        }

        public override Expression Visit(CollectionResourceFunctionCallNode nodeIn)
        {
            throw new NotSupportedException("CollectionResourceFunctionCallNode");
        }

        public override Expression Visit(CollectionFunctionCallNode nodeIn)
        {
            throw new NotSupportedException("CollectionFunctionCallNode");
        }

        public override Expression Visit(SingleValueOpenPropertyAccessNode nodeIn)
        {
            throw new NotSupportedException("SingleValueOpenPropertyAccessNode");
        }

        public override Expression Visit(SingleValuePropertyAccessNode nodeIn)
        {
            return TranslatePropertyAccess(nodeIn.Source, nodeIn.Property);
        }

        public override Expression Visit(UnaryOperatorNode nodeIn)
        {
            switch (nodeIn.OperatorKind)
            {
                case UnaryOperatorKind.Not:
                    return Expression.Not(this.TranslateNode(nodeIn.Operand));
                default:
                    throw new NotImplementedException();
            }
        }

        public override Expression Visit(NamedFunctionParameterNode nodeIn)
        {
            throw new NotSupportedException("NamedFunctionParameterNode");
        }

        public override Expression Visit(ParameterAliasNode nodeIn)
        {
            throw new NotSupportedException("ParameterAliasNode");
        }

        public override Expression Visit(SearchTermNode nodeIn)
        {
            throw new NotSupportedException("SearchTermNode");
        }

        /// <summary>
        /// entity.property
        /// </summary>
        /// <param name="nodeIn"></param>
        /// <returns></returns>
        public override Expression Visit(SingleComplexNode nodeIn)
        {
            throw new NotSupportedException("SingleComplexNode");
        }

        public override Expression Visit(CollectionComplexNode nodeIn)
        {
            throw new NotSupportedException("CollectionComplexNode");
        }

        public override Expression Visit(SingleValueCastNode nodeIn)
        {
            throw new NotSupportedException("SingleValueCastNode");
        }

        public override Expression Visit(AggregatedCollectionPropertyNode nodeIn)
        {
            throw new NotSupportedException("AggregatedCollectionPropertyNode");
        }

        public override Expression Visit(InNode nodeIn)
        {
            Expression singleValue = TranslateNode(nodeIn.Left);
            Expression collection = TranslateNode(nodeIn.Right);

            Type collectionItemType = collection.Type.GetElementType();
            if (collectionItemType == null)
            {
                Type[] genericArgs = collection.Type.GetGenericArguments();
                // The model builder does not support non-generic collections like ArrayList
                // or generic collections with generic arguments > 1 like IDictionary<,>
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

        internal static bool IsIQueryable(Type type)
        {
            return typeof(IQueryable).IsAssignableFrom(type);
        }


        private bool IsNullConstant(Expression expression)
        {
            return (expression.NodeType == ExpressionType.Constant && ((ConstantExpression)expression).Value == null);
        }

        private Expression TranslateComparison(BinaryOperatorKind operatorKind, Expression left, Expression right)
        {
            if (left.Type != right.Type)
            {
                if (IsNullConstant(left))
                {
                    left = Expression.Constant(null, right.Type);
                }
                else if (IsNullConstant(right))
                {
                    right = Expression.Constant(null, left.Type);
                }
                else if (left.Type.IsAssignableFrom(right.Type))
                {
                    right = Expression.Convert(right, left.Type);
                }
                else if (right.Type.IsAssignableFrom(left.Type))
                {
                    left = Expression.Convert(left, right.Type);
                }
                else
                {
                    throw new InvalidOperationException("Incompatible bianry operators");
                }
            }

            //TODO: Need to handle null value
            if (left.Type.IsEnum || left.Type.IsGenericType && left.Type.GetGenericArguments()[0].IsEnum)
            {
                Type enumType = left.Type;

                left = Expression.Convert(left, enumType.GetEnumUnderlyingType());
                right = Expression.Convert(right, enumType.GetEnumUnderlyingType());
            }

            switch (operatorKind)
            {
                case BinaryOperatorKind.Equal:
                    left = Expression.Equal(left, right);
                    break;
                case BinaryOperatorKind.NotEqual:
                    left = Expression.NotEqual(left, right);
                    break;
                case BinaryOperatorKind.GreaterThan:
                    left = Expression.GreaterThan(left, right);
                    break;
                case BinaryOperatorKind.GreaterThanOrEqual:
                    left = Expression.GreaterThanOrEqual(left, right);
                    break;
                case BinaryOperatorKind.LessThan:
                    left = Expression.LessThan(left, right);
                    break;
                case BinaryOperatorKind.LessThanOrEqual:
                    left = Expression.LessThanOrEqual(left, right);
                    break;
                case BinaryOperatorKind.Has:
                    left = Expression.Equal(Expression.And(left, right), right);
                    break;
            }

            return left;
        }

        public static LambdaExpression ApplyFilter(FilterClause filter, Type entityType)
        {
            ParameterExpression parameter = Expression.Parameter(entityType, "it");
            NodeToExpression nodeToExpressionTranslator = new NodeToExpression()
            {
                ImplicitVariableParameterExpression = parameter,
            };
            Expression filterNodeExpression = nodeToExpressionTranslator.TranslateNode(filter.Expression);

            // IQueryable<TSource> Where<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate);
            // translate to rootExpression.Where(filterNodeExpression)
            //return Expression.Call(
            //    typeof(Enumerable),
            //    "Where",
            //    new Type[] { entityType },
            //    Expression.Lambda(filterNodeExpression, parameter));
            return Expression.Lambda(filterNodeExpression, parameter);
        }
        public Expression TranslateNode(QueryNode node)
        {
            return node.Accept(this);
        }

        private Expression TranslatePropertyAccess(QueryNode sourceNode, IEdmProperty edmProperty)
        {
            Expression source = TranslateNode(sourceNode);
            Expression constantNullExpression = Expression.Constant(null);
            Expression propertyAccess = Expression.Property(source, edmProperty.Name);

            return Expression.Condition(
                Expression.Equal(source, constantNullExpression),
                Expression.Convert(constantNullExpression, propertyAccess.Type),
                propertyAccess);
        }

        private Expression TranslateFunctionCall(string functionName, IEnumerable<QueryNode> argumentNodes)
        {
            switch (functionName)
            {
                #region string functions
                case "contains":
                    var methodInfoOfContains = typeof(string).GetMethod("Contains", BindingFlags.Public | BindingFlags.Instance);
                    var instanceOfContains = argumentNodes.ElementAt(0).Accept(this);
                    var argumentOfContains = argumentNodes.ElementAt(1).Accept(this);
                    return Expression.Call(instanceOfContains, methodInfoOfContains, argumentOfContains);

                case "endswith":
                    Expression[] arguments = BindArguments(argumentNodes);
                    ValidateAllStringArguments(functionName, arguments);

                    return MakeFunctionCall(ClrCanonicalFunctions.EndsWith, arguments);

                case "startswith":
                    var methodInfoOfStartsWith = typeof(string).GetMethod("StartsWith", new Type[] { typeof(string) });
                    var instanceOfStartsWith = argumentNodes.ElementAt(0).Accept(this);
                    var argumentOfStartsWith = argumentNodes.ElementAt(1).Accept(this);
                    return Expression.Call(instanceOfStartsWith, methodInfoOfStartsWith, argumentOfStartsWith);

                case "length":
                    var propertyInfoOfLength = typeof(string).GetProperty("Length", typeof(int));
                    var instanceOfLength = argumentNodes.ElementAt(0).Accept(this);
                    return Expression.Property(instanceOfLength, propertyInfoOfLength);

                case "indexof":
                    var methodInfoOfIndexOf = typeof(string).GetMethod("IndexOf", new Type[] { typeof(string) });
                    var instanceOfIndexOf = argumentNodes.ElementAt(0).Accept(this);
                    var argumentOfIndexOf = argumentNodes.ElementAt(1).Accept(this);
                    return Expression.Call(instanceOfIndexOf, methodInfoOfIndexOf, argumentOfIndexOf);

                case "substring":
                    var argumentCount = argumentNodes.Count();
                    if (argumentNodes.Count() == 2)
                    {
                        var methodInfoOfSubString = typeof(string).GetMethod("Substring", new Type[] { typeof(int) });
                        var instanceOfSubString = argumentNodes.ElementAt(0).Accept(this);
                        var argumentOfSubString = argumentNodes.ElementAt(1).Accept(this);
                        return Expression.Call(instanceOfSubString, methodInfoOfSubString, argumentOfSubString);
                    }
                    else if (argumentNodes.Count() == 3)
                    {
                        var methodInfoOfSubString = typeof(string).GetMethod("Substring", new Type[] { typeof(int), typeof(int) });
                        var instanceOfSubString = argumentNodes.ElementAt(0).Accept(this);
                        var argumentOfSubString = argumentNodes.ElementAt(1).Accept(this);
                        var argumentOfSubString2 = argumentNodes.ElementAt(2).Accept(this);
                        return Expression.Call(instanceOfSubString, methodInfoOfSubString, argumentOfSubString, argumentOfSubString2);
                    }
                    else
                    {
                        throw new ArgumentException("argumentNodes");
                    }

                case "tolower":
                    var methodInfoOfToLower = typeof(string).GetMethod("ToLower", new Type[] { });
                    var instanceOfToLower = argumentNodes.ElementAt(0).Accept(this);
                    return Expression.Call(instanceOfToLower, methodInfoOfToLower);

                case "toupper":
                    var methodInfoOfToUpper = typeof(string).GetMethod("ToUpper", new Type[] { });
                    var instanceOfToUpper = argumentNodes.ElementAt(0).Accept(this);
                    return Expression.Call(instanceOfToUpper, methodInfoOfToUpper);

                case "trim":
                    var methodInfoOfTrim = typeof(string).GetMethod("Trim", new Type[] { });
                    var instanceOfTrim = argumentNodes.ElementAt(0).Accept(this);
                    return Expression.Call(instanceOfTrim, methodInfoOfTrim);

                case "concat":
                    var methodInfoOfConcat = typeof(string).GetMethod("Concat", new Type[] { typeof(string), typeof(string) });
                    var argumentOfConcat1 = argumentNodes.ElementAt(0).Accept(this);
                    var argumentOfConcat2 = argumentNodes.ElementAt(1).Accept(this);
                    return Expression.Call(methodInfoOfConcat, argumentOfConcat1, argumentOfConcat2);
                #endregion

                #region DateTime Method
                case "year":
                case "month":
                case "day":
                case "hour":
                case "minute":
                case "second":
                case "fractionalseconds":
                case "date":
                case "time":
                case "totaloffsetminutes":
                case "now":
                case "mindatetime":
                case "maxdatetime":
                    throw new NotSupportedException(functionName);
                #endregion

                #region Math Methods
                case "round":
                case "floor":
                case "ceiling":
                    throw new NotSupportedException(functionName);
                #endregion

                #region Type Functions
                case "cast":
                case "isof":
                    throw new NotSupportedException(functionName);
                #endregion

                #region Geo Functions
                case "geo.distance":
                case "geo.length":
                case "geo.intersects":
                    throw new NotSupportedException(functionName);
                #endregion
                default:
                    throw new ArgumentException(functionName);
            }
        }

        protected Expression[] BindArguments(IEnumerable<QueryNode> nodes)
        {
            return nodes.OfType<SingleValueNode>().Select(n => TranslateNode(n)).ToArray();
        }
        public Expression Visit(AllToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(AnyToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(BinaryOperatorToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(CountSegmentToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(InToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(DottedIdentifierToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(ExpandToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(ExpandTermToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(FunctionCallToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(LambdaToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(LiteralToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(InnerPathToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(OrderByToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(EndPathToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(CustomQueryOptionToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(RangeVariableToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(SelectToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(SelectTermToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(StarToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(UnaryOperatorToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(FunctionParameterToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(AggregateToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(AggregateExpressionToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(EntitySetAggregateToken tokenIn)
        {
            throw new NotImplementedException();
        }

        public Expression Visit(GroupByToken tokenIn)
        {
            throw new NotImplementedException();
        }

        private static void ValidateAllStringArguments(string functionName, Expression[] arguments)
        {
            if (arguments.Any(arg => arg.Type != typeof(string)))
            {
                throw new ArgumentException("Expecting string");
            }
        }

        // creates an expression for the corresponding OData function.
        internal Expression MakeFunctionCall(MemberInfo member, params Expression[] arguments)
        {

            IEnumerable<Expression> functionCallArguments = arguments;

            // if the argument is of type Nullable<T>, then translate the argument to Nullable<T>.Value as none
            // of the canonical functions have overloads for Nullable<> arguments.
            functionCallArguments = ExtractValueFromNullableArguments(functionCallArguments);

            Expression functionCall;
            if (member.MemberType == MemberTypes.Method)
            {
                MethodInfo method = member as MethodInfo;
                if (method.IsStatic)
                {
                    functionCall = Expression.Call(null, method, functionCallArguments);
                }
                else
                {
                    functionCall = Expression.Call(functionCallArguments.First(), method, functionCallArguments.Skip(1));
                }
            }
            else
            {
                // property
                functionCall = Expression.Property(functionCallArguments.First(), member as PropertyInfo);
            }

            return CreateFunctionCallWithNullPropagation(functionCall, arguments);
        }
        private static IEnumerable<Expression> ExtractValueFromNullableArguments(IEnumerable<Expression> arguments)
        {
            return arguments.Select(arg => ExtractValueFromNullableExpression(arg));
        }

        internal static Expression ExtractValueFromNullableExpression(Expression source)
        {
            return Nullable.GetUnderlyingType(source.Type) != null ? Expression.Property(source, "Value") : source;
        }

        internal Expression CreateFunctionCallWithNullPropagation(Expression functionCall, Expression[] arguments)
        {
            //if (QuerySettings.HandleNullPropagation == HandleNullPropagationOption.True)
            //{
            //    Expression test = CheckIfArgumentsAreNull(arguments);

            //    if (test == FalseConstant)
            //    {
            //        // none of the arguments are/can be null.
            //        // so no need to do any null propagation
            //        return functionCall;
            //    }
            //    else
            //    {
            //        // if one of the arguments is null, result is null (not defined)
            //        return
            //            Expression.Condition(
            //            test: test,
            //            ifTrue: Expression.Constant(null, ToNullable(functionCall.Type)),
            //            ifFalse: ToNullable(functionCall));
            //    }
            //}
            //else
            {
                return functionCall;
            }
        }
    }
}
