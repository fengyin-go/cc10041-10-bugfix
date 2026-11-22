<template>
  <section class="dag-panel" :data-testid="`dag-panel-${moduleKey}`">
    <header class="dag-panel-head">
      <div>
        <h3>依赖校验与运行</h3>
        <p class="page-desc">
          启动前先解析依赖，确认可执行分区；成环节点及其下游分支不写数据。
        </p>
      </div>
      <div class="page-actions">
        <input v-model="dagName" class="dag-input" placeholder="DAG 名称（留空取已发布）" />
        <button class="btn" data-testid="dag-plan" type="button" :disabled="busy" @click="onPlan">
          解析依赖
        </button>
        <template v-if="canRun">
          <button class="btn primary" data-testid="dag-run" type="button" :disabled="busy" @click="onRun">
            执行 DAG
          </button>
          <button class="btn" data-testid="dag-retry" type="button" :disabled="busy || !lastRunId" @click="onRetry">
            重试上次运行
          </button>
        </template>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="dag-error">{{ errorMessage }}</p>

    <div v-if="plan" class="dag-summary" data-testid="dag-plan-summary">
      <span :class="['badge', plan.has_cycle ? 'badge-danger' : 'badge-ok']">
        {{ plan.has_cycle ? '依赖成环' : '依赖无环' }}
      </span>
      <span class="legend-item">可执行分区：{{ plan.executable_partitions.join('、') || '无' }}</span>
      <span class="legend-item">阻塞分区：{{ plan.blocked_partitions.join('、') || '无' }}</span>
      <span v-if="plan.cycle_nodes.length" class="legend-item">
        环内节点：{{ plan.cycle_nodes.join('、') }}
      </span>
    </div>

    <table v-if="plan" class="data-table" data-testid="dag-plan-partitions">
      <thead>
        <tr>
          <th>分区</th>
          <th>节点</th>
          <th>可执行节点</th>
          <th>阻塞节点</th>
          <th>已产出目标</th>
          <th>未产出目标</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="part in plan.partitions" :key="part.name" :data-status="part.status">
          <td>{{ part.name }}</td>
          <td>{{ part.nodes.join('、') }}</td>
          <td>{{ part.executable_nodes.join('、') || '—' }}</td>
          <td>{{ part.blocked_nodes.join('、') || '—' }}</td>
          <td>{{ producedTargets(part).join('、') || '—' }}</td>
          <td>{{ (part.unproduced_targets ?? []).join('、') || '—' }}</td>
          <td>
            <span :class="['badge', partitionOk(part) ? 'badge-ok' : 'badge-warn']">
              {{ part.status }}
            </span>
          </td>
        </tr>
      </tbody>
    </table>

    <template v-if="report">
      <div class="dag-summary" data-testid="dag-audit-summary">
        <span class="legend-item">运行：{{ report.run_id }}</span>
        <span class="legend-item">状态：{{ report.status }}</span>
        <span class="legend-item">校验通过：{{ report.summary.passed }}</span>
        <span class="legend-item">未产出：{{ report.summary.failed }}</span>
        <span class="legend-item">实际写入目标：{{ report.summary.written_targets.join('、') || '无' }}</span>
      </div>

      <table class="data-table" data-testid="dag-audit-table">
        <thead>
          <tr>
            <th>管道名称</th>
            <th>输出目标</th>
            <th>分区</th>
            <th>实际写入行数</th>
            <th>校验结果</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in report.audits"
            :key="row.id"
            :data-testid="`audit-${row.管道名称}`"
            :data-status="row.校验结果"
          >
            <td>{{ row.管道名称 }}</td>
            <td>{{ row.输出目标 || '—' }}</td>
            <td>{{ row.partition || '—' }}</td>
            <td>{{ row.输出行数 }}</td>
            <td>
              <span :class="['badge', row.校验结果 === '校验通过' ? 'badge-ok' : 'badge-danger']">
                {{ row.校验结果 }}
              </span>
            </td>
            <td>{{ row.阻塞原因 || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <div v-if="history.length" class="dag-history">
      <h4>分区产出历史（按目标 {{ historyTarget || '全部' }}）</h4>
      <table class="data-table">
        <thead>
          <tr><th>运行</th><th>分区</th><th>状态</th><th>已产出目标</th><th>未产出目标</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in history" :key="`${row.run_id}-${row.partition_id}`" :data-status="row.status">
            <td>{{ row.run_id }}</td>
            <td>{{ row.name }}</td>
            <td>
              <span :class="['badge', row.confirmed ? 'badge-ok' : 'badge-warn']">{{ row.status }}</span>
            </td>
            <td>{{ row.output_targets.join('、') || '—' }}</td>
            <td>{{ row.unproduced_targets.join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="runs.length" class="dag-history">
      <h4>最近运行</h4>
      <ul class="run-list">
        <li v-for="run in runs.slice(-5).reverse()" :key="run.run_id" :data-run="run.run_id">
          <button class="link" type="button" @click="inspectRun(run.run_id)">
            {{ run.run_id }}
          </button>
          <span>{{ run.status }}</span>
          <span v-if="run.retry_of">重试自 {{ run.retry_of }}</span>
          <span v-if="run.has_cycle">成环：{{ (run.cycle_nodes ?? []).join('、') }}</span>
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  loadAudit,
  loadPartitionHistory,
  loadRuns,
  planDag,
  retryDag,
  runDag,
  type AuditReport,
  type DagPartition,
  type BatchRun,
  type DagPlan,
  type PartitionHistoryRow,
} from '@/api/backend-service'

const props = defineProps<{ moduleKey: string }>()

const canRun = computed(() => props.moduleKey !== 'output_audit')

function producedTargets(part: DagPartition): string[] {
  return part.output_targets?.length
    ? part.output_targets
    : (part.intended_output_targets ?? [])
}

function partitionOk(part: DagPartition): boolean {
  if (typeof part.confirmed === 'boolean') return part.confirmed
  return part.status === '可执行'
}

const dagName = ref('')
const plan = ref<DagPlan | null>(null)
const report = ref<AuditReport | null>(null)
const runs = ref<BatchRun[]>([])
const history = ref<PartitionHistoryRow[]>([])
const historyTarget = ref('')
const lastRunId = ref('')
const busy = ref(false)
const errorMessage = ref('')

function handleError(error: unknown, fallback: string) {
  errorMessage.value = error instanceof Error ? error.message : fallback
}

async function refreshRuns() {
  try {
    runs.value = (await loadRuns()).runs
  } catch (error) {
    handleError(error, '运行记录读取失败')
  }
}

async function applyRun(run: BatchRun) {
  lastRunId.value = run.run_id
  if (run.partitions?.length) {
    plan.value = {
      found: true,
      dag_name: run.dag_name,
      has_cycle: Boolean(run.has_cycle),
      cycles: run.cycles ?? [],
      cycle_nodes: run.cycle_nodes ?? [],
      executable_nodes: run.executable_nodes ?? [],
      blocked_nodes: run.blocked_nodes ?? [],
      execution_order: [],
      partitions: run.partitions,
      executable_partitions: run.partitions.filter((p) => p.confirmed).map((p) => p.name),
      blocked_partitions: run.partitions.filter((p) => !p.confirmed).map((p) => p.name),
      partition_count: run.partitions.length,
    }
  }
  report.value = await loadAudit(run.run_id)
  const unproduced = report.value.partitions
    .flatMap((part) => part.unproduced_targets)
    .find(Boolean)
  if (unproduced) {
    historyTarget.value = unproduced
    history.value = (await loadPartitionHistory(unproduced)).partitions
  }
  await refreshRuns()
}

async function onPlan() {
  busy.value = true
  errorMessage.value = ''
  try {
    plan.value = await planDag(dagName.value)
    report.value = null
  } catch (error) {
    handleError(error, '依赖解析失败')
  } finally {
    busy.value = false
  }
}

async function onRun() {
  busy.value = true
  errorMessage.value = ''
  try {
    const run = await runDag(dagName.value)
    await applyRun(run)
  } catch (error) {
    handleError(error, 'DAG 执行失败')
  } finally {
    busy.value = false
  }
}

async function onRetry() {
  if (!lastRunId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const run = await retryDag(lastRunId.value, dagName.value)
    await applyRun(run)
  } catch (error) {
    handleError(error, '重试失败')
  } finally {
    busy.value = false
  }
}

async function inspectRun(runId: string) {
  busy.value = true
  errorMessage.value = ''
  try {
    report.value = await loadAudit(runId)
    lastRunId.value = runId
    const unproduced = report.value.partitions
      .flatMap((part) => part.unproduced_targets)
      .find(Boolean)
    if (unproduced) {
      historyTarget.value = unproduced
      history.value = (await loadPartitionHistory(unproduced)).partitions
    }
  } catch (error) {
    handleError(error, '审计读取失败')
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  await refreshRuns()
  if (props.moduleKey === 'pipeline_dag') {
    await onPlan()
  } else if (runs.value.length) {
    await inspectRun(runs.value[runs.value.length - 1].run_id)
  }
})
</script>
