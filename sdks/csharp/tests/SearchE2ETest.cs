using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Flapjack.Search.Clients;
using Flapjack.Search.Models.Search;
using Flapjack.Search.Transport;
using Xunit;

namespace Flapjack.Search.Tests;

public class SearchE2ETest : IAsyncLifetime
{
    private SearchClient _client;
    private const string TestIndex = "test_csharp_e2e";
    private const string AppId = "test-app";
    private const string ApiKey = "test-api-key";
    private const string Host = "localhost";
    private const int Port = 7700;
    private static readonly IReadOnlyList<string> MutationTaskOwnerMethods = new[]
    {
        nameof(InitializeAsync),
        nameof(TestPartialUpdate),
        nameof(TestSaveAndDeleteObject),
        nameof(TestUpdateSettings),
        nameof(TestSynonyms),
        nameof(TestRules),
        nameof(TestMultiIndex),
    };
    private static readonly IReadOnlyDictionary<short, OpCode> OpCodesByValue = typeof(OpCodes)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(field => field.FieldType == typeof(OpCode))
        .Select(field => (OpCode)field.GetValue(null)!)
        .ToDictionary(opCode => opCode.Value);

    [Fact]
    public void TestMutationsDoNotUseFixedTaskDelayWaits()
    {
        AssertMutationMethodsAvoidTaskDelayCalls();
    }

    [Fact]
    public void TestMutationDelayGuardDetectsHelperWrappedTaskDelay()
    {
        var helperWrappedMethod = typeof(DelayGuardFixture).GetMethod(
            nameof(DelayGuardFixture.OwnerMethodCallingHelperAsync),
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly
        );
        Assert.NotNull(helperWrappedMethod);

        Assert.True(
            MethodContainsTaskDelayCall(helperWrappedMethod!),
            "Delay guard must catch Task.Delay even when the owner method delegates to same-class helpers."
        );
    }

    private static void AssertMutationMethodsAvoidTaskDelayCalls()
    {
        var ownerMethods = MutationTaskOwnerMethods
            .Select(name => typeof(SearchE2ETest).GetMethod(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .ToList();
        var missingMethods = MutationTaskOwnerMethods.Where((_, index) => ownerMethods[index] is null).ToList();
        Assert.True(missingMethods.Count == 0, $"Could not resolve expected mutation methods: {string.Join(", ", missingMethods)}");

        var methodsUsingTaskDelay = ownerMethods
            .Where(method => method is not null)
            .Select(method => method!)
            .Where(MethodContainsTaskDelayCall)
            .Select(method => method.Name)
            .ToList();
        Assert.True(
            methodsUsingTaskDelay.Count == 0,
            $"Mutation-readiness methods must use TaskID polling, not fixed sleeps. Found Task.Delay in: {string.Join(", ", methodsUsingTaskDelay)}"
        );
    }

    private static bool MethodContainsTaskDelayCall(MethodInfo method)
    {
        var methodsToInspect = new Queue<MethodBase>(GetMethodsForExecutableInspection(method));
        var inspectedMethods = new HashSet<int>();

        while (methodsToInspect.Count > 0)
        {
            var currentMethod = methodsToInspect.Dequeue();
            if (!inspectedMethods.Add(currentMethod.MetadataToken))
            {
                continue;
            }

            var helperMethods = new List<MethodBase>();
            if (MethodBodyCallsTaskDelay(method.DeclaringType, currentMethod, helperMethods))
            {
                return true;
            }

            foreach (var helperMethod in helperMethods)
            {
                foreach (var executableMethod in GetMethodsForExecutableInspection(helperMethod))
                {
                    if (!inspectedMethods.Contains(executableMethod.MetadataToken))
                    {
                        methodsToInspect.Enqueue(executableMethod);
                    }
                }
            }
        }

        return false;
    }

    private static IEnumerable<MethodBase> GetMethodsForExecutableInspection(MethodBase method)
    {
        yield return method;

        var asyncStateMachine = method switch
        {
            MethodInfo methodInfo => methodInfo.GetCustomAttribute<AsyncStateMachineAttribute>()?.StateMachineType,
            _ => null,
        };
        if (asyncStateMachine is null)
        {
            yield break;
        }

        var moveNextMethod = asyncStateMachine.GetMethod("MoveNext", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (moveNextMethod is not null)
        {
            yield return moveNextMethod;
        }
    }

    private static bool MethodBodyCallsTaskDelay(Type rootDeclaringType, MethodBase method, ICollection<MethodBase> helperMethods)
    {
        var ilBytes = method.GetMethodBody()?.GetILAsByteArray();
        if (ilBytes is null || ilBytes.Length == 0)
        {
            return false;
        }

        var offset = 0;
        while (offset < ilBytes.Length)
        {
            if (!TryReadOpCode(ilBytes, ref offset, out var opCode))
            {
                break;
            }

            if (opCode.OperandType == OperandType.InlineMethod && offset + sizeof(int) <= ilBytes.Length)
            {
                var metadataToken = BitConverter.ToInt32(ilBytes, offset);
                var resolvedMethod = ResolveMethodFromToken(method, metadataToken);
                if (resolvedMethod is not null && IsTaskDelayMethod(resolvedMethod))
                {
                    return true;
                }

                if (resolvedMethod is not null && ShouldFollowHelperMethod(rootDeclaringType, resolvedMethod))
                {
                    helperMethods.Add(resolvedMethod);
                }
            }

            offset += OperandSize(opCode.OperandType, ilBytes, offset);
        }

        return false;
    }

    private static bool TryReadOpCode(byte[] ilBytes, ref int offset, out OpCode opCode)
    {
        opCode = default;
        if (offset >= ilBytes.Length)
        {
            return false;
        }

        var rawValue = ilBytes[offset++];
        var opcodeValue = rawValue == 0xFE
            ? (short)((rawValue << 8) | ilBytes[offset++])
            : (short)rawValue;

        return OpCodesByValue.TryGetValue(opcodeValue, out opCode);
    }

    private static int OperandSize(OperandType operandType, byte[] ilBytes, int operandOffset)
    {
        return operandType switch
        {
            OperandType.InlineNone => 0,
            OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
            OperandType.InlineVar => 2,
            OperandType.InlineI or OperandType.InlineBrTarget or OperandType.InlineField or OperandType.InlineMethod or OperandType.InlineSig or OperandType.InlineString or OperandType.InlineTok or OperandType.InlineType or OperandType.ShortInlineR => 4,
            OperandType.InlineI8 or OperandType.InlineR => 8,
            OperandType.InlineSwitch => 4 + (BitConverter.ToInt32(ilBytes, operandOffset) * 4),
            _ => 0,
        };
    }

    private static bool ShouldFollowHelperMethod(Type rootDeclaringType, MethodBase helperMethod)
    {
        if (rootDeclaringType is null || helperMethod.DeclaringType is null)
        {
            return false;
        }

        if (helperMethod.DeclaringType == rootDeclaringType)
        {
            return true;
        }

        return helperMethod.DeclaringType.DeclaringType == rootDeclaringType
            && helperMethod.DeclaringType.Name.StartsWith("<", StringComparison.Ordinal);
    }

    private static bool IsTaskDelayMethod(MethodBase resolvedMethod)
    {
        return resolvedMethod.DeclaringType == typeof(Task) && resolvedMethod.Name == nameof(Task.Delay);
    }

    private static MethodBase ResolveMethodFromToken(MethodBase ownerMethod, int metadataToken)
    {
        try
        {
            var genericTypeArguments = ownerMethod.DeclaringType?.IsGenericType == true
                ? ownerMethod.DeclaringType.GetGenericArguments()
                : null;
            var genericMethodArguments = ownerMethod.IsGenericMethod
                ? ownerMethod.GetGenericArguments()
                : null;
            return ownerMethod.Module.ResolveMethod(metadataToken, genericTypeArguments, genericMethodArguments);
        }
        catch
        {
            return null;
        }
    }

    private sealed class DelayGuardFixture
    {
        public async Task OwnerMethodCallingHelperAsync()
        {
            await HelperMethodUsingDelayAsync();
        }

        private static async Task HelperMethodUsingDelayAsync()
        {
            await Task.Delay(1);
        }
    }

    private async Task WaitForTaskCompletionAsync(string indexName, long taskId)
    {
        await _client.WaitForTaskAsync(indexName, taskId);
    }

    private async Task WaitForTaskCompletionAsync(string indexName, long? taskId, string operationName)
    {
        if (!taskId.HasValue)
        {
            throw new InvalidOperationException($"{operationName} did not return a task identifier.");
        }

        await WaitForTaskCompletionAsync(indexName, taskId.Value);
    }

    public async Task InitializeAsync()
    {
        var config = new SearchConfig(AppId, ApiKey);
        config.CustomHosts = new List<StatefulHost>
        {
            new()
            {
                Url = Host,
                Port = Port,
                Scheme = HttpScheme.Http,
                Up = true,
                LastUse = DateTime.UtcNow,
                Accept = CallType.Read | CallType.Write,
            }
        };
        _client = new SearchClient(config);

        // Configure index settings for filtering and faceting
        var settings = new IndexSettings
        {
            SearchableAttributes = new List<string> { "name", "brand", "category" },
            AttributesForFaceting = new List<string> { "brand", "category", "price" }
        };
        var setSettingsResponse = await _client.SetSettingsAsync(TestIndex, settings);
        await WaitForTaskCompletionAsync(TestIndex, setSettingsResponse.TaskID);

        // Seed test data using batch
        var records = new List<BatchRequest>
        {
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "1"}, {"name", "iPhone 15 Pro"}, {"brand", "Apple"}, {"price", 999}, {"category", "electronics"}
            }),
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "2"}, {"name", "Samsung Galaxy S24"}, {"brand", "Samsung"}, {"price", 899}, {"category", "electronics"}
            }),
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "3"}, {"name", "Google Pixel 8"}, {"brand", "Google"}, {"price", 699}, {"category", "electronics"}
            }),
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "4"}, {"name", "MacBook Air M3"}, {"brand", "Apple"}, {"price", 1099}, {"category", "computers"}
            }),
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "5"}, {"name", "iPad Pro"}, {"brand", "Apple"}, {"price", 799}, {"category", "tablets"}
            }),
        };

        var batchResponse = await _client.BatchAsync(TestIndex, new BatchWriteParams(records));
        await WaitForTaskCompletionAsync(TestIndex, batchResponse.TaskID);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task TestListIndices()
    {
        var response = await _client.ListIndicesAsync();
        Assert.NotNull(response);
        Assert.NotNull(response.Items);
    }

    [Fact]
    public async Task TestBasicSearch()
    {
        var searchParams = new SearchParams(new SearchParamsObject { Query = "iPhone" });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.NotNull(response.Hits);
        Assert.True(response.Hits.Count > 0, "Expected at least one hit for 'iPhone'");
    }

    [Fact]
    public async Task TestEmptyQuery()
    {
        var searchParams = new SearchParams(new SearchParamsObject { Query = "" });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.True(response.Hits.Count >= 5, "Expected all records returned for empty query");
    }

    [Fact]
    public async Task TestFilters()
    {
        var searchParams = new SearchParams(new SearchParamsObject
        {
            Query = "",
            Filters = "brand:Apple"
        });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.True(response.Hits.Count >= 2, "Expected at least 2 Apple products");
    }

    [Fact]
    public async Task TestFacets()
    {
        var searchParams = new SearchParams(new SearchParamsObject
        {
            Query = "",
            Facets = new List<string> { "brand", "category" }
        });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.NotNull(response.Facets);
        Assert.True(response.Facets.ContainsKey("brand"), "Expected 'brand' facet");
    }

    [Fact]
    public async Task TestHighlighting()
    {
        var searchParams = new SearchParams(new SearchParamsObject { Query = "iPhone" });
        var response = await _client.SearchSingleIndexAsync<Dictionary<string, object>>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.True(response.Hits.Count > 0);
        var firstHit = response.Hits[0];
        Assert.True(firstHit.ContainsKey("_highlightResult"), "Expected highlighting in results");
    }

    [Fact]
    public async Task TestPagination()
    {
        var searchParams = new SearchParams(new SearchParamsObject
        {
            Query = "",
            HitsPerPage = 2,
            Page = 0
        });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
        Assert.True(response.Hits.Count <= 2, "Expected at most 2 hits per page");

        // Get second page
        var page2Params = new SearchParams(new SearchParamsObject
        {
            Query = "",
            HitsPerPage = 2,
            Page = 1
        });
        var response2 = await _client.SearchSingleIndexAsync<object>(TestIndex, page2Params);
        Assert.NotNull(response2);
        Assert.True(response2.Hits.Count > 0, "Expected hits on second page");
    }

    [Fact]
    public async Task TestGetObject()
    {
        var response = await _client.GetObjectAsync(TestIndex, "1");
        Assert.NotNull(response);
    }

    [Fact]
    public async Task TestPartialUpdate()
    {
        var updateResponse = await _client.PartialUpdateObjectAsync(
            TestIndex,
            "1",
            new Dictionary<string, object> { { "price", 1099 } }
        );
        await WaitForTaskCompletionAsync(TestIndex, updateResponse.TaskID, "PartialUpdateObjectAsync");

        var obj = await _client.GetObjectAsync(TestIndex, "1");
        Assert.NotNull(obj);
    }

    [Fact]
    public async Task TestSaveAndDeleteObject()
    {
        var newObj = new Dictionary<string, object>
        {
            {"objectID", "temp-csharp-100"},
            {"name", "Temporary Test Object"},
            {"brand", "TestBrand"}
        };

        // Save using batch for reliability
        var saveResponse = await _client.BatchAsync(TestIndex, new BatchWriteParams(new List<BatchRequest>
        {
            new(Models.Search.Action.AddObject, newObj)
        }));
        await WaitForTaskCompletionAsync(TestIndex, saveResponse.TaskID);

        // Verify saved
        var saved = await _client.GetObjectAsync(TestIndex, "temp-csharp-100");
        Assert.NotNull(saved);

        // Delete
        var deleteResponse = await _client.DeleteObjectAsync(TestIndex, "temp-csharp-100");
        await WaitForTaskCompletionAsync(TestIndex, deleteResponse.TaskID);

        // Verify deleted
        var ex = await Assert.ThrowsAsync<Exceptions.FlapjackApiException>(async () =>
        {
            await _client.GetObjectAsync(TestIndex, "temp-csharp-100");
        });
        Assert.Contains("404", ex.HttpErrorCode.ToString());
    }

    [Fact]
    public async Task TestGetSettings()
    {
        var settings = await _client.GetSettingsAsync(TestIndex);
        Assert.NotNull(settings);
    }

    [Fact]
    public async Task TestUpdateSettings()
    {
        var newSettings = new IndexSettings
        {
            SearchableAttributes = new List<string> { "name", "brand" }
        };
        var updateSettingsResponse = await _client.SetSettingsAsync(TestIndex, newSettings);
        await WaitForTaskCompletionAsync(TestIndex, updateSettingsResponse.TaskID);

        var settings = await _client.GetSettingsAsync(TestIndex);
        Assert.NotNull(settings);
        Assert.NotNull(settings.SearchableAttributes);
        Assert.Contains("name", settings.SearchableAttributes);
        Assert.Contains("brand", settings.SearchableAttributes);
    }

    [Fact]
    public async Task TestSynonyms()
    {
        var synonym = new SynonymHit("syn-phone", SynonymType.Synonym)
        {
            Synonyms = new List<string> { "phone", "mobile", "cell" }
        };
        var saveSynonymResponse = await _client.SaveSynonymAsync(TestIndex, "syn-phone", synonym);
        await WaitForTaskCompletionAsync(TestIndex, saveSynonymResponse.TaskID);

        var saved = await _client.GetSynonymAsync(TestIndex, "syn-phone");
        Assert.NotNull(saved);
        Assert.Equal("syn-phone", saved.ObjectID);
    }

    [Fact]
    public async Task TestRules()
    {
        var rule = new Rule("rule-promo", new Consequence
        {
            Params = new ConsequenceParams { Filters = "brand:Apple" }
        })
        {
            Conditions = new List<Condition>
            {
                new() { Pattern = "promo", Anchoring = Anchoring.Contains }
            }
        };
        var saveRuleResponse = await _client.SaveRuleAsync(TestIndex, "rule-promo", rule);
        await WaitForTaskCompletionAsync(TestIndex, saveRuleResponse.TaskID);

        var saved = await _client.GetRuleAsync(TestIndex, "rule-promo");
        Assert.NotNull(saved);
        Assert.Equal("rule-promo", saved.ObjectID);
    }

    [Fact]
    public async Task TestUserAgent()
    {
        // Verify client was initialized — user agent is set internally
        var searchParams = new SearchParams(new SearchParamsObject { Query = "test" });
        var response = await _client.SearchSingleIndexAsync<object>(TestIndex, searchParams);
        Assert.NotNull(response);
    }

    [Fact]
    public async Task TestMultiIndex()
    {
        // Seed a second index
        var secondIndex = "test_csharp_e2e_multi";
        var secondIndexBatchResponse = await _client.BatchAsync(secondIndex, new BatchWriteParams(new List<BatchRequest>
        {
            new(Models.Search.Action.AddObject, new Dictionary<string, object>
            {
                {"objectID", "m1"}, {"title", "Multi Index Test"}
            })
        }));
        await WaitForTaskCompletionAsync(secondIndex, secondIndexBatchResponse.TaskID);

        // Search both indices
        var result1 = await _client.SearchSingleIndexAsync<object>(TestIndex, new SearchParams(new SearchParamsObject { Query = "" }));
        var result2 = await _client.SearchSingleIndexAsync<object>(secondIndex, new SearchParams(new SearchParamsObject { Query = "" }));
        Assert.NotNull(result1);
        Assert.NotNull(result2);
        Assert.True(result1.Hits.Count > 0, "Expected hits from first index");
        Assert.True(result2.Hits.Count > 0, "Expected hits from second index");
    }
}
