import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

type EventRow = Record<string, string | number | boolean | null>
type FinalRow = Record<string, string | number | null>

const ENDPOINT = '/api/heartbeat'

const eventColumns = [
  '事件编号',
  '节点编号',
  '采集时间',
  '租约秒数',
  '处理状态',
  '租约版本',
  '批次版本',
  '处理说明',
]

const finalColumns = [
  '节点编号',
  '原因',
  '原因码',
  '判定时间',
  '租约到期时间',
  '记录时间',
]

const reasonOptions = [
  { code: '', label: '全部原因' },
  { code: 'manual_offline', label: '人工离线' },
  { code: 'lease_expired', label: '租约到期' },
  { code: 'slot_snapshot_failed', label: '槽位快照失败' },
]

export default function HeartbeatView() {
  const [events, setEvents] = useState<EventRow[]>([])
  const [eventTotal, setEventTotal] = useState(0)
  const [finals, setFinals] = useState<FinalRow[]>([])
  const [finalTotal, setFinalTotal] = useState(0)
  const [nodeCode, setNodeCode] = useState('')
  const [eventStatus, setEventStatus] = useState('')
  const [reasonCode, setReasonCode] = useState('')
  const [summary, setSummary] = useState<Record<string, unknown>>({})
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const reload = useCallback(async () => {
    try {
      const eventQuery = new URLSearchParams()
      if (nodeCode) eventQuery.set('node_code', nodeCode)
      if (eventStatus) eventQuery.set('status', eventStatus)
      const finalQuery = new URLSearchParams()
      if (nodeCode) finalQuery.set('node_code', nodeCode)
      if (reasonCode) finalQuery.set('reason_code', reasonCode)
      const [eventResponse, finalResponse, summaryResponse] = await Promise.all([
        request(`${ENDPOINT}?${eventQuery.toString()}`),
        request(`${ENDPOINT}/final-records?${finalQuery.toString()}`),
        request('/api/domain/summary'),
      ])
      if (!eventResponse.ok || !finalResponse.ok) {
        throw new Error('心跳数据读取失败')
      }
      const eventPayload = await eventResponse.json()
      const finalPayload = await finalResponse.json()
      setEvents(eventPayload.items ?? [])
      setEventTotal(eventPayload.total ?? 0)
      setFinals(finalPayload.items ?? [])
      setFinalTotal(finalPayload.total ?? 0)
      setSummary(await summaryResponse.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '心跳租约数据读取失败')
    }
  }, [nodeCode, eventStatus, reasonCode])

  useEffect(() => {
    void reload()
  }, [reload])

  async function post(path: string, body: Record<string, unknown> = {}) {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload?.detail ?? '动作未生效')
      }
      setMessage(String(payload.message ?? '动作已提交'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '动作失败')
    }
  }

  async function escalateSweep() {
    await post(`${ENDPOINT}/sweep`)
  }

  const pendingCount = Number(summary['待处理心跳'] ?? 0)
  const schedulableCount = Number(summary['可调度节点数'] ?? 0)

  return (
    <section className="page" data-module="heartbeat">
      <header className="page-head">
        <div>
          <h2>节点心跳租约</h2>
          <p className="page-desc">
            采集心跳事件、校验槽位快照并延长租约；离线最终记录按原因码区分租约到期与人工离线。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void post(`${ENDPOINT}/ingest`)}>
            处理待处理心跳
          </button>
          <button className="btn" type="button" onClick={() => void escalateSweep()}>
            扫描到期租约
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">心跳事件数</span>
          <strong className="stat-value">{eventTotal}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">待处理心跳</span>
          <strong className="stat-value">{pendingCount}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">可调度节点</span>
          <strong className="stat-value">{schedulableCount}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">离线最终记录</span>
          <strong className="stat-value">{finalTotal}</strong>
        </article>
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void reload()
        }}
      >
        <label className="filter-item">
          <span>节点编号</span>
          <input value={nodeCode} placeholder="按节点编号检索" onChange={(event) => setNodeCode(event.target.value)} />
        </label>
        <label className="filter-item">
          <span>处理状态</span>
          <select value={eventStatus} onChange={(event) => setEventStatus(event.target.value)}>
            <option value="">全部状态</option>
            <option value="待处理">待处理</option>
            <option value="已处理">已处理</option>
            <option value="已合并">已合并</option>
            <option value="迟到">迟到</option>
            <option value="已拒绝">已拒绝</option>
          </select>
        </label>
        <label className="filter-item">
          <span>离线原因</span>
          <select value={reasonCode} onChange={(event) => setReasonCode(event.target.value)}>
            {reasonOptions.map((option) => (
              <option key={option.code} value={option.code}>{option.label}</option>
            ))}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
        <button
          className="btn ghost"
          type="button"
          onClick={() => {
            setNodeCode('')
            setEventStatus('')
            setReasonCode('')
          }}
        >
          重置条件
        </button>
      </form>

      <h3 className="section-title">心跳事件</h3>
      <table className="data-table">
        <thead>
          <tr>{eventColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {events.map((row) => (
            <tr key={String(row.id)}>
              {eventColumns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {events.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={eventColumns.length}>暂无心跳事件，可在采集节点执行心跳上报</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">离线最终记录</h3>
      <table className="data-table">
        <thead>
          <tr>{finalColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {finals.map((row) => (
            <tr key={String(row.id)}>
              {finalColumns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {finals.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={finalColumns.length}>暂无离线最终记录</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {eventTotal} 条心跳事件 / {finalTotal} 条离线最终记录</span>
        {message && <span>{message}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

function formatCell(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'boolean') return value ? '是' : '否'
  return String(value)
}
