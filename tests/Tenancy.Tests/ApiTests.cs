using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Tenancy.Tests;

public class ApiTests
{
    private static WebApplicationFactory<Program> Factory(params (string Key, string Value)[] settings)
        => new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Downstream:LatencyMs", "5");
            b.UseSetting("Downstream:JitterMs", "0");
            foreach (var (key, value) in settings) b.UseSetting(key, value);
        });

    [Fact]
    public async Task Sem_tenant_retorna_400()
    {
        await using var app = Factory();
        var res = await app.CreateClient().GetAsync("/api/cotacao?skus=1,2");
        Assert.Equal(HttpStatusCode.BadRequest, res.StatusCode);
    }

    [Fact]
    public async Task Tenant_desconhecido_retorna_404()
    {
        await using var app = Factory();
        var client = app.CreateClient();
        client.DefaultRequestHeaders.Add("X-Tenant-Id", "rede-99");
        var res = await client.GetAsync("/api/cotacao?skus=1,2");
        Assert.Equal(HttpStatusCode.NotFound, res.StatusCode);
    }

    [Fact]
    public async Task Cada_rede_tem_a_sua_tabela_de_precos()
    {
        await using var app = Factory();
        var a = await Quote(app, "rede-01");
        var b = await Quote(app, "rede-02");

        Assert.Equal("rede-01", a.Tenant);
        Assert.Equal(5, a.Itens.Length);
        Assert.NotEqual(a.Total, b.Total); // mesmos SKUs, preços diferentes por tenant
    }

    [Fact]
    public async Task Com_admissao_ligada_recusa_rapido_o_que_nao_cabe_em_3_segundos()
    {
        await using var app = Factory(
            ("Replica:Slots", "1"),
            ("Downstream:LatencyMs", "2000"),
            ("Admission:Enabled", "true"),
            ("Admission:BudgetMs", "3000"));
        var client = app.CreateClient();
        client.DefaultRequestHeaders.Add("X-Tenant-Id", "rede-01");

        var first = client.GetAsync("/api/cotacao?skus=1");   // ocupa o único slot por 2 s
        await WaitUntil(async () => (await Metrics(client)).InFlight == 1);
        var second = client.GetAsync("/api/cotacao?skus=2");  // ninguém na fila ainda: 0 + 2 s cabe em 2,7 s
        await WaitUntil(async () => (await Metrics(client)).Queued == 1);

        var started = DateTime.UtcNow;
        var third = await client.GetAsync("/api/cotacao?skus=3"); // 1 na frente: 2 s + 2 s > 2,7 s
        var elapsed = DateTime.UtcNow - started;

        Assert.Equal(HttpStatusCode.ServiceUnavailable, third.StatusCode);
        Assert.Equal("1", third.Headers.GetValues("Retry-After").Single());
        Assert.True(elapsed < TimeSpan.FromMilliseconds(500), $"A recusa deveria ser imediata, levou {elapsed.TotalMilliseconds} ms");

        Assert.Equal(HttpStatusCode.OK, (await first).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await second).StatusCode);
    }

    private static async Task<CotacaoDto> Quote(WebApplicationFactory<Program> app, string tenant)
    {
        var client = app.CreateClient();
        client.DefaultRequestHeaders.Add("X-Tenant-Id", tenant);
        var res = await client.GetAsync("/api/cotacao?skus=10,20,30,40,50");
        res.EnsureSuccessStatusCode();
        return (await res.Content.ReadFromJsonAsync<CotacaoDto>())!;
    }

    private static async Task<MetricsDto> Metrics(HttpClient client)
        => (await client.GetFromJsonAsync<MetricsDto>("/metrics"))!;

    private static async Task WaitUntil(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condição não atingida.");
            await Task.Delay(20);
        }
    }

    private sealed record ItemDto(int Sku, decimal Preco, int Estoque);
    private sealed record CotacaoDto(string Tenant, string Replica, ItemDto[] Itens, decimal Total);
    private sealed record MetricsDto(string ReplicaId, int InFlight, int Queued, int Slots);
}
