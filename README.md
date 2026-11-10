# Benchmark .NET: autoscaling multi-tenant com resposta em até 3 s

Uma API de cotação para **10 redes de farmácia (tenants)** recebe um pico de **2.000 usuários simultâneos**. A regra de negócio é responder em **até 3 segundos**. O projeto mede o que acontece com:

| Versão | Estratégia |
|---|---|
| v1 | 1 réplica fixa |
| v2 | 6 réplicas fixas, dimensionadas para o pico e ligadas o tempo todo |
| v3 | Autoscaling reativo (estilo HPA): mínimo 2, máximo 10 |
| v4 | Autoscaling + controle de admissão pelo prazo de 3 s |
| v5a | Vizinho barulhento: uma rede dispara uma integração sem pausa, fila FIFO |
| v5b | O mesmo, com fila justa por tenant (round-robin) |

Tudo roda com o .NET 8 puro, sem pacotes de terceiros na API, no gateway e no gerador de carga.

## Resultados

Medidos em uma máquina Linux de 2 núcleos (Ubuntu 24.04, .NET 8). Relatório completo, linha do tempo a cada 5 s e eventos de escala em [`results/`](results/RESULTS.md).

### Dia de campanha (v1 a v4)

Perfil de carga: 300 usuários por 30 s, sobe para 2.000 em 10 s (o push da campanha), fica 90 s no pico e volta para 300.

| Versão | Dentro de 3 s (teste todo) | p95 no pico | Tempo com SLA quebrado | Réplicas no pico | Réplicas no vale | Réplica-segundos |
|---|---:|---:|---:|---:|---:|---:|
| v1 · 1 réplica | 32,5% | 8.155 ms | 115 s | 1 | 1 | 180 |
| v2 · 6 fixas | 100% | 297 ms | 0 s | 6 | 6 | 1.080 |
| v3 · autoscaling | 96,5% | 297 ms | 10 s | 10 → 6 | 2 | 1.090 |
| v4 · autoscaling + admissão | 98,7% (100% das aceitas) | 298 ms | 15 s | 10 → 6 | 2 | 1.080 |

O que aconteceu no v3, segundo a segundo (`results/v3-autoscaling.gateway-status.json`):

- 30 s: começa o pico. 2 réplicas dão conta de ~400 req/s; o pico pede ~890 req/s.
- 35,5 s: o autoscaler pede 4 réplicas novas.
- 40 a 50 s: só ~22% das respostas saem em até 3 s.
- 51 s: as réplicas novas ficam prontas (15,6 s depois do pedido).
- 56 s: chega a 10 réplicas. Precisava de 6: a fila acumulada inflou a métrica e o autoscaler pediu demais.
- 85 s e 115 s: desce para 7 e depois 6, respeitando a janela de estabilização.
- 165 s: no vale, volta para 2.

No v4, a réplica estima a espera da fila antes de aceitar. Se a resposta não cabe em 3 s (menos uma margem de 300 ms), devolve **503 com `Retry-After: 1` em 3 ms (p95)**. Resultado: 1.201 recusas rápidas no lugar de respostas lentas, e **100% das requisições aceitas responderam em até 3 s** (máximo de 2,6 s).

### Vizinho barulhento (v5)

Teto de 4 réplicas, 2.000 usuários constantes: 1.000 usuários de 9 redes (2 s de pausa entre ações) e uma integração de uma única rede com 1.000 conexões disparando sem pausa.

| Fila | Usuários: p95 | Usuários: cotações atendidas | Integração: p95 | Integração: cotações atendidas |
|---|---:|---:|---:|---:|
| v5a · FIFO | 1.921 ms | 28.762 | 1.920 ms | 65.709 |
| v5b · justa por tenant | **321 ms** | **46.684** (+62%) | 3.040 ms | 47.570 |

Com FIFO, a integração ocupa a fila e todo mundo espera junto. Com a fila justa, cada tenant que tem alguém esperando é atendido uma vez por rodada. Quem gerou o pico paga pelo próprio pico.

## Como funciona

```
LoadGen (2.000 usuários virtuais)
   │  X-Tenant-Id: rede-01..rede-10
   ▼
Gateway (proxy reverso, menos requisições pendentes)  ◄── Autoscaler (a cada 5 s)
   │                                                        lê /metrics das réplicas
   ▼                                                        sobe/derruba processos
Réplicas da API (processos separados)
   ├─ resolve o tenant (400 sem cabeçalho, 404 desconhecido)
   ├─ controle de admissão (v4)
   ├─ TenantScheduler: 50 slots por réplica, fila FIFO ou justa
   └─ cotação: chamada de estoque/ERP (250 ms ± 50 ms) + preço do tenant
```

- **`src/Tenancy.Core`**: `TenantScheduler` (slots + fila FIFO ou justa por tenant), `AdmissionControl` (estimativa de espera contra o orçamento) e `ScalingPolicy` (cálculo de réplicas no estilo HPA: desejado = teto(carga / alvo), sobe no máximo para o dobro ou +4, desce só depois da janela de estabilização).
- **`src/Tenancy.Api`**: a réplica. `GET /api/cotacao?skus=...` com `X-Tenant-Id`, `/metrics` e `/health/ready`.
- **`src/Tenancy.Gateway`**: proxy reverso, gerenciador de réplicas (um "mini Kubernetes" que sobe processos da API, espera o readiness e drena antes de desligar) e o autoscaler.
- **`tools/LoadGen`**: usuários virtuais em laço fechado (faz a cotação, espera a resposta, pausa e repete), perfil de carga em estágios, relatório em JSON e Markdown.

### Parâmetros do modelo

| Parâmetro | Valor | Por quê |
|---|---|---|
| Slots por réplica | 50 | A capacidade de um pod (pool de conexões/threads). Com 250 ms por cotação, ~200 req/s por réplica |
| Chamada externa | 250 ms ± 50 ms | Estoque/ERP. É I/O: a thread fica livre, o slot fica ocupado |
| Pausa do usuário | 2 s ± 25% | Tempo de ler a tela antes da próxima ação |
| Alvo do autoscaler | 40 por réplica | 80% dos 50 slots, como um HPA com alvo de 80% |
| Avaliação | a cada 5 s | |
| Provisionamento | 15 s | Agendar o pod e puxar a imagem, com nó já disponível. Com nó novo, costuma passar de 1 minuto |
| Estabilização na descida | 30 s | |
| Orçamento de tempo | 3.000 ms, margem de 300 ms | |

## Rodando

Pré-requisito: .NET 8 SDK.

```bash
dotnet test                        # testes automatizados
scripts/run-benchmark.sh           # todos os cenários (~17 min), relatórios em results/
ONLY=v5a-vizinho-barulhento-fifo,v5b-vizinho-barulhento-fila-justa scripts/run-benchmark.sh
PEAK=500 BASE=100 scripts/run-benchmark.sh   # rodada menor
```

O workflow do GitHub Actions roda os testes a cada push. Para rodar o benchmark no Actions e gravar os resultados no repositório, use "Run workflow" ou coloque `[benchmark]` na mensagem do commit.

## Testes

- O scheduler nunca passa do limite de slots.
- FIFO atende por ordem de chegada; a fila justa intercala os tenants.
- Cancelar na fila libera a vez e não deixa contagem para trás.
- A estimativa de fila da fila justa considera só o rodízio.
- Admissão: fila vazia admite; recusa quando a espera estimada estoura o orçamento.
- Política de escala: cálculo do desejado, limite de subida, mínimo/máximo e janela de estabilização.
- API: 400 sem tenant, 404 para tenant desconhecido, preços diferentes por rede e recusa imediata (503 + `Retry-After`) do que não cabe em 3 s.

## Limitações

- A chamada externa é simulada com atraso fixo. Num sistema real, o banco e as dependências também saturam quando as réplicas sobem.
- Réplicas são processos na mesma máquina, não pods em nós diferentes. O tempo de provisionamento é um parâmetro.
- Os números são de uma máquina de 2 núcleos. Em outra máquina os valores absolutos mudam; a comparação entre as versões é o que importa.
