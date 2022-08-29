using Microsoft.OData.UriParser;
using Microsoft.OData.UriParser.Aggregation;

namespace FilterPoc.Shared
{
    public class Node1
    {
        public static Node1 Null = new Node1(NodeValueType.Null);
        public NodeValueType NodeType { get; }
        public NodeValueType CollectionItemType { get; }
        public enum NodeValueType
        {
            Null,
            Bool,
            String,
            Int,
            Collection,
            Group,
        }
        public bool IsNull() => NodeType == NodeValueType.Null;
        public bool IsString() => NodeType == NodeValueType.String;

        /// <summary>
        /// Group is special, it is an array but syntax acts as a string
        /// </summary>
        /// <returns></returns>
        public bool IsGroup() => NodeType == NodeValueType.Group;
        public bool AllowIntOperations() => NodeType == NodeValueType.Int || IsIntCollection();
        public bool AllowStringOperations() => IsString() || IsGroup();
        public object? Value { get; }
        public Node1(NodeValueType type, object? value)
        {
            NodeType = type;
            Value = value;
        }
        public Node1(IEnumerable<object> value, NodeValueType itemType)
        {
            NodeType = NodeValueType.Collection;
            CollectionItemType = itemType;
            Value = value;
        }
        public Node1(NodeValueType type)
        {
            NodeType = type;
        }

        public bool AsBoolean()
        {
            if (NodeType == NodeValueType.Bool)
            {
                return (bool)Value;
            }

            if (NodeType == NodeValueType.Collection && CollectionItemType == NodeValueType.Bool)
            {
                // any true is true
                return AsCollection<bool>().Any(s => s);
            }

            throw new ArgumentException(NodeType.ToString());
        }

        public int AsInt()
        {
            if (NodeType == NodeValueType.Int)
            {
                return (int)Value;
            }
            throw new ArgumentException(NodeType.ToString());
        }

        public bool IsIntCollection() => NodeType == NodeValueType.Collection && CollectionItemType == NodeValueType.Int;

        public IEnumerable<T> AsCollection<T>()
        {
            if (NodeType == NodeValueType.Collection)
            {
                return ((IEnumerable<object>)Value).Select(s => (T)s);
            }
            throw new ArgumentException(NodeType.ToString());
        }

        public string AsString()
        {
            if (NodeType == NodeValueType.String)
            {
                return (string)Value;
            }

            throw new ArgumentException(NodeType.ToString());
        }

        public string[] AsGroup()
        {
            if (NodeType == NodeValueType.Group)
            {
                return (string[])Value;
            }

            throw new ArgumentException(NodeType.ToString());
        }

        public static implicit operator Node1(bool b) => new Node1(NodeValueType.Bool, b);
        public static implicit operator Node1(int b) => new Node1(NodeValueType.Int, b);
        public static implicit operator Node1(string b) => new Node1(NodeValueType.String, b);
    }

    class ConnectionNodeMatcher : ISyntacticTreeVisitor<Node1>
    {
        private readonly Connection _connection;

        public ConnectionNodeMatcher(Connection connection)
        {
            _connection = connection;
        }

        public static bool Matches(Connection connection, QueryToken token)
        {
            var matcher = new ConnectionNodeMatcher(connection);
            return token.Accept(matcher).AsBoolean();
        }

        public Node1 ParseToken(QueryToken token)
        {
            return token.Accept(this);
        }

        public bool IntOperation(BinaryOperatorKind kind, int a, int b)
        {
            switch (kind)
            {
                case BinaryOperatorKind.GreaterThan:
                    return a > b;
                case BinaryOperatorKind.GreaterThanOrEqual:
                    return a >= b;
                case BinaryOperatorKind.LessThan:
                    return a < b;
                case BinaryOperatorKind.LessThanOrEqual:
                    return a <= b;
            }

            throw new NotSupportedException(kind.ToString());
        }

        // string | boolean 
        public Node1 Visit(BinaryOperatorToken tokenIn)
        {
            var left = ParseToken(tokenIn.Left);
            var right = ParseToken(tokenIn.Right);
            switch (tokenIn.OperatorKind)
            {
                case BinaryOperatorKind.Or:
                    return left.AsBoolean() || right.AsBoolean();
                case BinaryOperatorKind.And:
                    return left.AsBoolean() && right.AsBoolean();
                case BinaryOperatorKind.Equal:
                    return left.Value == right.Value;
                case BinaryOperatorKind.NotEqual:
                    return left.Value != right.Value;
                case BinaryOperatorKind.GreaterThan:
                case BinaryOperatorKind.GreaterThanOrEqual:
                case BinaryOperatorKind.LessThan:
                case BinaryOperatorKind.LessThanOrEqual:
                    if (left.IsNull() || right.IsNull())
                    {
                        return false;
                    }
                    {
                        if (left.AllowIntOperations() && right.AllowIntOperations())
                        {
                            if (left.IsIntCollection())
                            {
                                if (right.IsIntCollection())
                                {
                                    throw new ArgumentException(tokenIn.ToString());
                                }

                                return left.AsCollection<int>().Any(g => IntOperation(tokenIn.OperatorKind, g, right.AsInt()));
                            }

                            if (right.IsIntCollection())
                            {
                                return right.AsCollection<int>().Any(g => IntOperation(tokenIn.OperatorKind, left.AsInt(), g));
                            }

                            return IntOperation(tokenIn.OperatorKind, left.AsInt(), right.AsInt());
                        }
                        throw new NotSupportedException(tokenIn.OperatorKind.ToString());
                    }
                case BinaryOperatorKind.Add:
                case BinaryOperatorKind.Subtract:
                case BinaryOperatorKind.Multiply:
                case BinaryOperatorKind.Divide:
                case BinaryOperatorKind.Modulo:
                case BinaryOperatorKind.Has:
                default:
                    throw new NotSupportedException(tokenIn.OperatorKind.ToString());
            }
        }

        private string[] ParseInCollection(string literal)
        {
            if (literal.Length <= 2 ||
                !(literal[0] == '(' && literal[^1] == ')'))
            {
                throw new ArgumentException(literal, nameof(literal));
            }


            return literal[1..^1].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        }

        public Node1 Visit(InToken tokenIn)
        {
            var left = ParseToken(tokenIn.Left);

            if (left.NodeType == Node1.NodeValueType.Null)
            {
                return false;
            }
            var right = ParseToken(tokenIn.Right);
            var collection = ParseInCollection(right.AsString());
            switch (left.NodeType)
            {
                case Node1.NodeValueType.String:
                    return collection.Contains(left.AsString());
                case Node1.NodeValueType.Group:
                    return left.AsGroup().Any(g => collection.Contains(g));
            }
            throw new ArgumentException(right.NodeType.ToString(), nameof(InToken));
        }

        private object MethodInvoke(string name, string first, string second)
        {
            switch (name)
            {
                case "contains":
                    return first.Contains(second);
                case "startswith":
                    return first.StartsWith(second);
                case "endswith":
                    return first.EndsWith(second);
                default:
                    throw new ArgumentException(name);
            }
        }

        private object MethodInvoke(string name, string first)
        {
            switch (name)
            {
                case "length":
                    return first.Length;
                case "tolower":
                    return first.ToLowerInvariant();
                case "toupper":
                    return first.ToUpperInvariant();
                case "trim":
                    return first.Trim();
                default:
                    throw new ArgumentException(name);
            }
        }

        private int TwoStringToNumberMethodInvoke(string name, string first, string second)
        {
            switch (name)
            {
                case "indexof":
                    return first.IndexOf(second);
                default:
                    throw new InvalidOperationException(name);
            }
        }

        private string SubstringInvoke(string first, int startIndex, int? length)
        {
            if (length == null)
            {
                return first.Substring(startIndex);
            }

            return first.Substring(startIndex, length.Value);
        }

        private Node1 StringToTMethodInvoke(string name, Node1.NodeValueType returnType, Node1 instanceToken)
        {
            if (instanceToken.IsNull())
            {
                return Node1.Null;
            }
            if (instanceToken.AllowStringOperations())
            {
                if (instanceToken.IsString())
                {
                    var item = instanceToken.AsString();
                    return new Node1(returnType, MethodInvoke(name, item));
                }
                else
                {
                    var group = instanceToken.AsGroup();
                    return new Node1(group.Select(g => MethodInvoke(name, g)), returnType);
                }
            }
            throw new ArgumentException(instanceToken.NodeType.ToString(), name);
        }

        private Node1 StringStringToTMethodInvoke(string name, Node1.NodeValueType returnType, Node1 firstNode, Node1 secondNode)
        {
            if (secondNode.IsNull() || firstNode.IsNull())
            {
                return false;
            }

            if (secondNode.IsString())
            {
                var second = secondNode.AsString();
                if (firstNode.IsString())
                {
                    var first = firstNode.AsString();
                    return new Node1(returnType, MethodInvoke(name, first, second));
                }
                if (firstNode.IsGroup())
                {
                    var first = firstNode.AsGroup();
                    return new Node1(first.Select(g => MethodInvoke(name, g, second)), returnType);
                }
            }

            if (secondNode.IsGroup())
            {
                if (firstNode.IsString())
                {
                    return new Node1(secondNode.AsGroup()
                        .Select(g =>
                            MethodInvoke(name, firstNode.AsString(), g)), returnType);
                }
            }

            throw new ArgumentException(returnType.ToString(), name);
        }

        public Node1 Visit(FunctionCallToken tokenIn)
        {
            switch (tokenIn.Name)
            {
                // (string)=>int
                case "length":
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        return StringToTMethodInvoke(tokenIn.Name, Node1.NodeValueType.Int, instanceToken);
                    }
                // (string)=>string
                case "tolower":
                case "toupper":
                case "trim":
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        return StringToTMethodInvoke(tokenIn.Name, Node1.NodeValueType.String, instanceToken);
                    }
                // (string,string)=>bool
                case "contains":
                case "startswith":
                case "endswith":
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        var argumentToken = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        return StringStringToTMethodInvoke(tokenIn.Name, Node1.NodeValueType.Bool, instanceToken, argumentToken);
                    }
                // (string,string)=>int
                case "indexof":
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        var argumentToken = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        return StringStringToTMethodInvoke(tokenIn.Name, Node1.NodeValueType.Int, instanceToken, argumentToken);
                    }
                // (string,string)=>string
                case "concat":
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        var argumentToken = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        return StringStringToTMethodInvoke(tokenIn.Name, Node1.NodeValueType.String, instanceToken, argumentToken);
                    }
                // (string,int)=>string
                // (string,int,int)=>string
                case "substring":
                    var argumentCount = tokenIn.Arguments.Count();
                    if (argumentCount == 2)
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        var argumentToken = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        throw new NotImplementedException();
                    }
                    else if (argumentCount == 3)
                    {
                        var instanceToken = ParseToken(tokenIn.Arguments.ElementAt(0).ValueToken);
                        var argumentToken1 = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        var argumentToken2 = ParseToken(tokenIn.Arguments.ElementAt(1).ValueToken);
                        throw new NotImplementedException();
                    }
                    else
                    {
                        throw new ArgumentException("argumentNodes");
                    }


                default:
                    break;
            }
            throw new NotImplementedException();
        }

        public Node1 Visit(LiteralToken tokenIn)
        {
            Node1.NodeValueType nodeType = Node1.NodeValueType.Null;
            switch (tokenIn.Value)
            {
                case bool:
                    nodeType = Node1.NodeValueType.Bool;
                    break;
                case int:
                    nodeType = Node1.NodeValueType.Int;
                    break;
                case null:
                    nodeType = Node1.NodeValueType.Null;
                    break;
                case string:
                    nodeType = Node1.NodeValueType.String;
                    break;
                default:
                    throw new InvalidOperationException(tokenIn.Value.GetType().Name);
            }
            return new Node1(nodeType, tokenIn.Value);
        }

        public Node1 Visit(EndPathToken tokenIn)
        {
            if (tokenIn.Identifier.Equals("userId", StringComparison.OrdinalIgnoreCase))
            {
                if (_connection.userId == null) return Node1.Null;
                return new Node1(Node1.NodeValueType.String, _connection.userId);
            }
            else if (tokenIn.Identifier.Equals("connectionId", StringComparison.OrdinalIgnoreCase))
            {
                if (_connection.connectionId == null)
                {
                    throw new ArgumentNullException(nameof(_connection.connectionId));
                }
                return new Node1(Node1.NodeValueType.String, _connection.connectionId);
            }
            else if (tokenIn.Identifier.Equals("group", StringComparison.OrdinalIgnoreCase))
            {
                if (_connection.groups == null || _connection.groups.Length == 0) return Node1.Null;
                return new Node1(Node1.NodeValueType.Group, _connection.groups);
            }
            throw new InvalidOperationException(tokenIn.Identifier);
        }

        public Node1 Visit(UnaryOperatorToken tokenIn)
        {
            switch (tokenIn.OperatorKind)
            {
                case UnaryOperatorKind.Not:
                    return !ParseToken(tokenIn.Operand).AsBoolean();
                default:
                    throw new NotSupportedException(tokenIn.OperatorKind.ToString());
            }
        }

        public Node1 Visit(RangeVariableToken tokenIn)
        {
            throw new NotSupportedException("RangeVariableToken");
        }

        public Node1 Visit(FunctionParameterToken tokenIn)
        {
            throw new NotSupportedException("FunctionParameterToken");
        }

        public Node1 Visit(AggregateToken tokenIn)
        {
            throw new NotSupportedException("AggregateToken");
        }

        public Node1 Visit(AggregateExpressionToken tokenIn)
        {
            throw new NotSupportedException("AggregateExpressionToken");
        }

        public Node1 Visit(AllToken tokenIn)
        {
            throw new NotSupportedException("all");
        }

        public Node1 Visit(AnyToken tokenIn)
        {
            throw new NotSupportedException("any");
        }
        public Node1 Visit(CountSegmentToken tokenIn)
        {
            throw new NotSupportedException("count");
        }

        public Node1 Visit(DottedIdentifierToken tokenIn)
        {
            throw new NotSupportedException(".");
        }

        public Node1 Visit(ExpandToken tokenIn)
        {
            throw new NotSupportedException("ExpandToken");
        }

        public Node1 Visit(ExpandTermToken tokenIn)
        {
            throw new NotSupportedException("ExpandTermToken");
        }

        public Node1 Visit(LambdaToken tokenIn)
        {
            throw new NotSupportedException("LambdaToken");
        }

        public Node1 Visit(InnerPathToken tokenIn)
        {
            throw new NotSupportedException("InnerPathToken");
        }

        public Node1 Visit(OrderByToken tokenIn)
        {
            throw new NotSupportedException("OrderByToken");
        }

        public Node1 Visit(CustomQueryOptionToken tokenIn)
        {
            throw new NotSupportedException("CustomQueryOptionToken");
        }

        public Node1 Visit(SelectToken tokenIn)
        {
            throw new NotSupportedException("select");
        }

        public Node1 Visit(SelectTermToken tokenIn)
        {
            throw new NotSupportedException("select");
        }

        public Node1 Visit(StarToken tokenIn)
        {
            throw new NotSupportedException("*");
        }

        public Node1 Visit(EntitySetAggregateToken tokenIn)
        {
            throw new NotSupportedException("EntitySetAggregateToken");
        }

        public Node1 Visit(GroupByToken tokenIn)
        {
            throw new NotSupportedException("GroupByToken");
        }
    }
}
