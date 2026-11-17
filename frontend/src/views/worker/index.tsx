import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/worker'
const columns = [
  "节点编号",
  "主机地址",
  "并发槽位数",
  "空闲槽位",
  "在线判断",
  "可调度",
  "上次心跳时间",
  "节点状态标签"
]
const actions = [
  "提交注册",
  "标记在线",
  "标记离线",
  "恢复在线",
  "归档"
]
const statuses = [
  "草稿",
  "注册中",
  "在线",
  "离线",
  "归档"
]
const statLabels = [
  { key: "工作节点总数", label: "工作节点总数" },
  { key: "在线节点数", label: "在线节点数" },
  { key: "可调度节点数", label: "可调度节点数" },
  { key: "离线节点数", label: "离线节点数" }
]

type Row = Record<string, string | number | boolean | null>

export default function WorkerView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [stats, setStats] = useState<Record<string, number>>({})
  const [errorMessage, setErrorMessage] = useState('')
  const [message, setMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [detail, setDetail] = useState<Record<string, unknown> | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [draft, setDraft] = useState({ 节点编号: '', 主机地址: '', 并发槽位数: '4', 地域标签: '' })
  const filterFields = useMemo(() => ['节点编号', '主机地址', 'status'], [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [listResponse, statsResponse] = await Promise.all([
        request(`${ENDPOINT}?${query}`),
        request(`${ENDPOINT}/stats`),
      ])
      if (!listResponse.ok) {
        throw new Error('工作节点列表读取失败')
      }
      const payload = await listResponse.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      if (statsResponse.ok) {
        setStats(await statsResponse.json())
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集节点列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        throw new Error(payload?.message ?? '采集节点动作未生效，请稍后重试')
      }
      setMessage(String(payload.message ?? '动作已生效'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '采集节点操作失败')
    }
  }

  async function openDetail(row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}`)
      if (!response.ok) throw new Error('节点明细读取失败')
      setDetail(await response.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '节点明细读取失败')
    }
  }

  async function reportHeartbeat(row: Row) {
    setErrorMessage('')
    setMessage('')
    try {
      const collect = await request('/api/heartbeat/collect', {
        method: 'POST',
        body: JSON.stringify({
          events: [{
            事件编号: `UI-${String(row['节点编号'])}-${Date.now()}`,
            节点编号: row['节点编号'],
            采集来源: '界面模拟上报',
          }],
        }),
      })
      const collected = await collect.json()
      if (!collect.ok || collected.ok === false) {
        throw new Error(collected?.message ?? '心跳事件未采集')
      }
      const ingest = await request('/api/heartbeat/ingest', { method: 'POST', body: '{}' })
      const ingested = await ingest.json()
      if (!ingest.ok) throw new Error(ingested?.detail ?? '心跳处理失败')
      setMessage(String(ingested.message ?? '心跳已处理'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '心跳上报失败')
    }
  }

  async function createNode() {
    setErrorMessage('')
    setMessage('')
    try {
      const response = await request(ENDPOINT, {
        method: 'POST',
        body: JSON.stringify({ values: draft }),
      })
      const payload = await response.json()
      if (!response.ok || payload.ok === false) {
        throw new Error(payload?.message ?? '登记未生效')
      }
      setMessage(String(payload.message ?? '已登记'))
      setShowCreate(false)
      setDraft({ 节点编号: '', 主机地址: '', 并发槽位数: '4', 地域标签: '' })
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '登记失败')
    }
  }

  const decisions = (detail?.['判定记录'] ?? []) as Record<string, unknown>[]
  const history = (detail?.['状态历史'] ?? []) as Record<string, unknown>[]

  return (
    <section className="page" data-module="worker">
      <header className="page-head">
        <div>
          <h2>采集节点管理</h2>
          <p className="page-desc">
            管理分布式采集集群的工作节点，跟踪资源使用和心跳健康状态；在线判断与可调度结果来自同一套租约判定。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setShowCreate((prev) => !prev)}>
            登记工作节点
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出采集节点清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {statLabels.map((item) => (
          <article key={item.key} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{stats[item.key] ?? 0}</strong>
          </article>
        ))}
      </div>

      {showCreate && (
        <form
          className="filter-bar"
          onSubmit={(event) => {
            event.preventDefault()
            void createNode()
          }}
        >
          {['节点编号', '主机地址', '并发槽位数', '地域标签'].map((field) => (
            <label key={field} className="filter-item">
              <span>{field}</span>
              <input
                value={(draft as Record<string, string>)[field] ?? ''}
                onChange={(event) => setDraft((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            </label>
          ))}
          <button className="btn primary" type="submit">保存登记</button>
        </form>
      )}

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field === 'status' ? '状态' : field}</span>
            {field === 'status' ? (
              <select
                value={filters[field] ?? ''}
                onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
              >
                <option value="">全部状态</option>
                {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
              </select>
            ) : (
              <input
                value={filters[field] ?? ''}
                placeholder={`按${field}检索`}
                onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            )}
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void openDetail(row)}>明细</button>
                <button className="link" type="button" onClick={() => void reportHeartbeat(row)}>心跳上报</button>
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无采集节点数据，可先登记工作节点</td>
            </tr>
          )}
        </tbody>
      </table>

      {detail && (
        <>
          <h3 className="section-title">
            节点 {String(detail['节点编号'])} 的判定链（状态 {String(detail['status'])} / {String(detail['在线判断'])}）
            <button className="link" type="button" onClick={() => setDetail(null)}>关闭</button>
          </h3>
          <table className="data-table">
            <thead>
              <tr><th>判定时间</th><th>优先级</th><th>结果</th><th>来源</th><th>租约版本</th><th>租约到期时间</th><th>原因</th></tr>
            </thead>
            <tbody>
              {decisions.map((item) => (
                <tr key={String(item['判定序号'])}>
                  <td>{formatCell(item['判定时间'] as string)}</td>
                  <td>{formatCell(item['判定优先级'] as string)}</td>
                  <td>{formatCell(item['结果'] as string)}</td>
                  <td>{formatCell(item['来源'] as string)}</td>
                  <td>{formatCell(item['租约版本'] as string)}</td>
                  <td>{formatCell(item['租约到期时间'] as string)}</td>
                  <td>{formatCell(item['原因'] as string)}</td>
                </tr>
              ))}
              {decisions.length === 0 && (
                <tr><td className="empty-state" colSpan={7}>尚无判定记录</td></tr>
              )}
            </tbody>
          </table>
          <table className="data-table">
            <thead>
              <tr><th>序号</th><th>时间</th><th>动作</th><th>原状态</th><th>新状态</th><th>操作人</th><th>原因</th></tr>
            </thead>
            <tbody>
              {history.map((item) => (
                <tr key={String(item['序号'])}>
                  <td>{formatCell(item['序号'] as string)}</td>
                  <td>{formatCell(item['时间'] as string)}</td>
                  <td>{formatCell(item['动作'] as string)}</td>
                  <td>{formatCell(item['原状态'] as string)}</td>
                  <td>{formatCell(item['新状态'] as string)}</td>
                  <td>{formatCell(item['操作人'] as string)}</td>
                  <td>{formatCell(item['原因'] as string)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <footer className="page-foot">
        <span>共 {total} 条采集节点记录</span>
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
