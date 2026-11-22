import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 批量DAG编排与输出审计
// ---------------------------------------------------------------------------

export type DagPartition = {
  name: string
  status: string
  nodes: string[]
  executable_nodes: string[]
  blocked_nodes: string[]
  cycle_nodes: string[]
  output_targets: string[]
  unproduced_targets: string[]
  intended_output_targets?: string[]
  reason: string
  confirmed?: boolean
  runtime_status?: string
  row_count?: number
}

export type DagPlan = {
  found: boolean
  reason?: string
  dag_name: string
  has_cycle: boolean
  cycles: string[][]
  cycle_nodes: string[]
  executable_nodes: string[]
  blocked_nodes: string[]
  execution_order: string[]
  partitions: DagPartition[]
  executable_partitions: string[]
  blocked_partitions: string[]
  partition_count: number
}

export type BatchRun = {
  run_id: string
  run_key?: string
  dag_name: string
  status: string
  has_cycle?: boolean
  cycles?: string[][]
  cycle_nodes?: string[]
  executable_nodes?: string[]
  blocked_nodes?: string[]
  tasks_completed: number
  blocked_tasks?: number
  total_tasks: number
  partitions?: DagPartition[]
  blocked_scope?: { run_id?: string; nodes?: string[]; targets?: string[] }
  previous_blocked_scope?: { run_id?: string; nodes?: string[]; targets?: string[] }
  retry_of?: string
  started_at?: string
  reused?: boolean
  idempotent_hit?: boolean
  reason?: string
}

export type AuditRow = {
  id: number
  管道名称: string
  输出目标: string
  输出时间: string
  输出行数: string
  校验结果: string
  status: string
  run_id: string
  partition?: string
  partition_status?: string
  已落地?: boolean
  阻塞原因?: string
}

export type AuditReport = {
  found: boolean
  run_id: string
  dag_name: string
  status: string
  has_cycle: boolean
  cycles: string[][]
  cycle_nodes: string[]
  partitions: DagPartition[]
  audits: AuditRow[]
  writes: { 管道名称?: string; 输出目标?: string; 输出行数?: number }[]
  summary: {
    audit_count: number
    passed: number
    failed: number
    written_targets: string[]
    passed_targets: string[]
    blocked_targets: string[]
    cycle_targets: string[]
    confirmed_partitions: string[]
    unproduced_partitions: string[]
  }
}

async function postJson<T>(path: string): Promise<T> {
  const response = await request(path, { method: 'POST' })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload?.detail ?? '执行失败')
  return payload as T
}

export async function planDag(dagName = ''): Promise<DagPlan> {
  const query = dagName ? `?dag_name=${encodeURIComponent(dagName)}` : ''
  return await postJson<DagPlan>(`/api/pipeline/batch/dag/plan${query}`)
}

export async function runDag(
  dagName = '',
  runKey = '',
  failNodes: string[] = [],
): Promise<BatchRun> {
  const params = new URLSearchParams()
  if (dagName) params.set('dag_name', dagName)
  if (runKey) params.set('run_key', runKey)
  if (failNodes.length) params.set('fail_nodes', failNodes.join(','))
  const query = params.toString()
  return await postJson<BatchRun>(
    `/api/pipeline/batch/dag/run${query ? `?${query}` : ''}`,
  )
}

export async function retryDag(
  previousRunId: string,
  dagName = '',
): Promise<BatchRun> {
  const params = new URLSearchParams({ previous_run_id: previousRunId })
  if (dagName) params.set('dag_name', dagName)
  return await postJson<BatchRun>(`/api/pipeline/batch/dag/retry?${params.toString()}`)
}

export async function loadRuns(): Promise<{ total: number; runs: BatchRun[] }> {
  const response = await request('/api/pipeline/batch/runs')
  if (!response.ok) throw new Error('运行记录读取失败')
  return await response.json()
}

export async function loadAudit(runId = ''): Promise<AuditReport> {
  const query = runId ? `?run_id=${encodeURIComponent(runId)}` : ''
  const response = await request(`/api/pipeline/batch/audit${query}`)
  if (!response.ok) throw new Error('审计报告读取失败')
  return await response.json()
}

export type PartitionHistoryRow = DagPartition & {
  run_id: string
  run_key?: string
  partition_id: string
  retry_of?: string
}

export async function loadPartitionHistory(
  target = '',
): Promise<{ total: number; partitions: PartitionHistoryRow[] }> {
  const query = target ? `?target=${encodeURIComponent(target)}` : ''
  const response = await request(`/api/pipeline/batch/partitions/history${query}`)
  if (!response.ok) throw new Error('分区历史读取失败')
  return await response.json()
}
