// See https://aka.ms/new-console-template for more information
using System.Linq.Expressions;
using System.Text.RegularExpressions;

using FilterPoc.Shared;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.OData.Edm;
using Microsoft.OData.ModelBuilder;
using Microsoft.OData.UriParser;

Console.WriteLine("Hello, World!");

var a = new A(async (s) =>
{
    await Task.Delay(5000);
    Console.WriteLine(s);
    throw new ArgumentException();
});

a.Complete("Complete");
Console.WriteLine("done");
return;

// "userId eq 'user1' and group eq 'group1'" 
// how to flattern group?
var filterQuery = "userId eq 'user1' or (not endswith(userId, 'test')) or group in ('group1', 'group2')";// and not 'group1' in group";
var filterQuery1 = "userId ne null";
var filterQuery2 = "length(group) gt length(userId)";
var filterQuery3 = "length(group) gt length(userId)";
var url = new Uri($"Connections?$filter={filterQuery}", UriKind.Relative);
var model = CreateModel();
var parser = new ODataUriParser(model, url);
parser.Resolver.EnableCaseInsensitive = true;
FilterClause filter = parser.ParseFilter();

var expression = NodeToExpression.ApplyFilter(filter, typeof(Connection));

UriQueryExpressionParser expressionParser = new UriQueryExpressionParser(100);
QueryToken filterToken = expressionParser.ParseFilter(filterQuery2);
var matches = ConnectionNodeMatcher.Matches(new Connection
{
    userId = "aaaaaaaa",
    groups = new string[] {
    "abtest", "a" 
    }
}, filterToken);
Console.WriteLine(matches);
Console.Read();
var models = new Connection[]
{
    new Connection
    {
        //userId = "test2",
        group = "group1"
    }
};
var result = expression.Compile().DynamicInvoke(models[0]);
var k = models.Where(s=>(bool)expression.Compile().DynamicInvoke(s));
Console.ReadLine();


IEdmModel CreateModel()
{
    var builder = new ODataModelBuilder();
    var connection = builder.EntityType<Connection>();
    connection.HasKey(c => c.connectionId);
    builder.EntitySet<Connection>("Connections");
    connection.Property(s => s.userId);
    connection.Property(s => s.group);
    connection.CollectionProperty(s => s.groups);
    return builder.GetEdmModel();
}

public class Connection
{
    public string userId { get; set; }
    public string connectionId { get; set; }
    public string group { get; set; }
    public string[] groups { get; set; }
}

public record A(Action<string> Complete);
