#!/usr/bin/env bash
# Roda todos os cenários do benchmark e grava os relatórios em results/.
#
# Perfil de carga dos cenários v1 a v4 (o "dia de campanha"):
#   0-30 s    300 usuários (movimento normal)
#   30-40 s   sobe para 2.000 usuários em 10 s (push da campanha)
#   40-130 s  2.000 usuários
#   130-140 s volta para 300
#   140-180 s 300 usuários
#
# Uso: scripts/run-benchmark.sh
#      PEAK=500 BASE=100 ONLY=v1-uma-replica scripts/run-benchmark.sh   (rodada rápida)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${OUT:-$ROOT/results}"
PEAK="${PEAK:-2000}"
BASE="${BASE:-300}"
THINK_MS="${THINK_MS:-2000}"
ONLY="${ONLY:-}"
BIN="$ROOT/artifacts"

PROFILE="0:$BASE,30:$BASE,40:$PEAK,130:$PEAK,140:$BASE,180:$BASE"
PROFILE_ARGS=(--users "$PEAK" --duration 180 --profile "$PROFILE" --steady-from 60 --steady-to 130)

echo "== build =="
dotnet publish "$ROOT/src/Tenancy.Api" -c Release -o "$BIN/api" --nologo -v q ${RESTORE_FLAGS:-}
dotnet publish "$ROOT/src/Tenancy.Gateway" -c Release -o "$BIN/gateway" --nologo -v q ${RESTORE_FLAGS:-}
dotnet publish "$ROOT/tools/LoadGen" -c Release -o "$BIN/loadgen" --nologo -v q ${RESTORE_FLAGS:-}
mkdir -p "$OUT"

# Cada réplica: 50 processamentos simultâneos; cada cotação chama o estoque/ERP (250 ms ± 50 ms).
COMMON=(
  "ReplicaSettings__Replica__Slots=50"
  "ReplicaSettings__Downstream__LatencyMs=250"
  "ReplicaSettings__Downstream__JitterMs=50"
  "ReplicaSettings__Admission__BudgetMs=3000"
)

# Autoscaler no estilo HPA: avalia a cada 5 s, alvo de 40 em uso por réplica (80% dos 50 slots),
# cada réplica nova leva 15 s para existir (agendar o pod e puxar a imagem) e a descida espera 30 s.
AUTOSCALING=(
  "Scaling__Enabled=true" "Scaling__InitialReplicas=2" "Scaling__MinReplicas=2" "Scaling__MaxReplicas=10"
  "Scaling__TargetLoadPerReplica=40" "Scaling__EvaluationIntervalSeconds=5"
  "Scaling__ProvisioningDelaySeconds=15" "Scaling__ScaleDownStabilizationSeconds=30"
)

run() {
  local name="$1"; shift
  local loadgen_args=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do loadgen_args+=("$1"); shift; done
  shift
  local gateway_env=("$@")
  if [[ -n "$ONLY" && ",$ONLY," != *",$name,"* ]]; then return; fi

  echo "== $name =="
  env "${COMMON[@]}" "${gateway_env[@]}" \
    Scaling__ApiAssemblyPath="$BIN/api/Tenancy.Api.dll" \
    dotnet "$BIN/gateway/Tenancy.Gateway.dll" --urls http://127.0.0.1:8080 > "$OUT/$name.gateway.log" 2>&1 &
  local gw=$!

  for _ in $(seq 1 100); do
    if curl -fs http://127.0.0.1:8080/gateway/status | grep -q '"readyReplicas":[1-9]'; then break; fi
    sleep 0.2
  done

  dotnet "$BIN/loadgen/LoadGen.dll" --name "$name" --target http://127.0.0.1:8080 \
    --think-ms "$THINK_MS" --out "$OUT/$name.json" "${loadgen_args[@]}"

  curl -fs http://127.0.0.1:8080/gateway/status > "$OUT/$name.gateway-status.json" || true
  kill "$gw" 2>/dev/null || true
  wait "$gw" 2>/dev/null || true
  sleep 3
}

# v1: uma réplica fixa
run v1-uma-replica "${PROFILE_ARGS[@]}" -- Scaling__Enabled=false Scaling__InitialReplicas=1

# v2: réplicas fixas dimensionadas para o pico (ligadas o tempo todo)
run v2-fixas-no-pico "${PROFILE_ARGS[@]}" -- Scaling__Enabled=false Scaling__InitialReplicas=6

# v3: autoscaling reativo
run v3-autoscaling "${PROFILE_ARGS[@]}" -- "${AUTOSCALING[@]}"

# v4: autoscaling + controle de admissão pelo prazo de 3 s
run v4-autoscaling-admissao "${PROFILE_ARGS[@]}" -- "${AUTOSCALING[@]}" ReplicaSettings__Admission__Enabled=true

# v5: vizinho barulhento. Teto de 4 réplicas e 2.000 usuários constantes, metade deles é
# uma integração de uma única rede disparando sem pausa.
NOISY_ARGS=(--users "$PEAK" --ramp 30 --duration 120 --steady-from 60 --noisy-users $(( PEAK / 2 )) --noisy-tenant rede-01)
run v5a-vizinho-barulhento-fifo "${NOISY_ARGS[@]}" -- Scaling__Enabled=false Scaling__InitialReplicas=4 ReplicaSettings__Queue__Mode=Fifo
run v5b-vizinho-barulhento-fila-justa "${NOISY_ARGS[@]}" -- Scaling__Enabled=false Scaling__InitialReplicas=4 ReplicaSettings__Queue__Mode=FairPerTenant

echo "== resumo =="
dotnet "$BIN/loadgen/LoadGen.dll" --summarize "$OUT" > "$OUT/RESULTS.md"
echo "Relatórios em $OUT"
