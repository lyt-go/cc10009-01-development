import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/routing'

const rosterColumns = ['目标域名', '节点编号', '状态', '并发槽位数', '空闲槽位', '源批次版本', '加入时间', '退出时间', '退出原因']
const assignmentColumns = ['请求编号', '目标域名', '目标URL', '节点编号', '处置', '转派次数', '源批次版本', '说明', '分配时间']
const syncColumns = ['同步版本', '批次版本', '可调度节点数', '目标域名数', '转派请求数', '待处理请求数', '同步时间']

export default function RoutingView() {
  const [roster, setRoster] = useState<Row[]>([])
  const [assignments, setAssignments] = useState<Row[]>([])
  const [syncs, setSyncs] = useState<Row[]>([])
  const [domain, setDomain] = useState('news.example.com')
  const [requestCode, setRequestCode] = useState('REQ-1001')
  const [targetUrl, setTargetUrl] = useState('https://news.example.com/article/1')
  const [verifyResult, setVerifyResult] = useState<Record<string, unknown> | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const reload = useCallback(async () => {
    try {
      const query = domain ? `?domain=${encodeURIComponent(domain)}` : ''
      const [rosterResponse, assignmentResponse, syncResponse] = await Promise.all([
        request(`${ENDPOINT}/roster${query}`),
        request(`${ENDPOINT}/assignments${query}`),
        request(`${ENDPOINT}/syncs`),
      ])
      if (!rosterResponse.ok || !assignmentResponse.ok || !syncResponse.ok) {
        throw new Error('请求分流数据读取失败')
      }
      const rosterPayload = await rosterResponse.json()
      const assignmentPayload = await assignmentResponse.json()
      const syncPayload = await syncResponse.json()
      setRoster(rosterPayload.items ?? [])
      setAssignments([...(assignmentPayload.items ?? [])].reverse())
      setSyncs(syncPayload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分流数据读取失败')
    }
  }, [domain])

  useEffect(() => {
    void reload()
  }, [reload])

  async function submitRequest() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/requests`, {
        method: 'POST',
        body: JSON.stringify({
          requests: [{ 请求编号: requestCode, 目标域名: domain, 目标URL: targetUrl }],
        }),
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload?.detail ?? '新增请求未写入')
      setMessage(String(payload.message ?? '已写入'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '新增请求未写入')
    }
  }

  async function syncRoster() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/sync`, { method: 'POST', body: JSON.stringify({}) })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload?.detail ?? '同步失败')
      setMessage(String(payload.message ?? '已同步'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '同步失败')
    }
  }

  async function verify() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/verify`)
      if (!response.ok) throw new Error('校验接口返回异常')
      const payload = await response.json()
      setVerifyResult(payload)
      setMessage(payload.ok ? '分流侧一致性校验通过' : '分流侧一致性校验未通过')
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '校验失败')
    }
  }

  const checks = (verifyResult?.['校验项'] ?? []) as { 校验项: string; 通过: boolean; 详情: string }[]

  return (
    <section className="page" data-module="routing">
      <header className="page-head">
        <div>
          <h2>请求分流侧</h2>
          <p className="page-desc">
            读取同一份可调度节点结果，把目标域名的新增请求写入分流侧；节点退出时请求转派或保留待处理，不静默丢弃。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" type="button" onClick={() => void syncRoster()}>同步可调度结果</button>
          <button className="btn" type="button" onClick={() => void verify()}>一致性校验</button>
        </div>
      </header>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void submitRequest()
        }}
      >
        <label className="filter-item">
          <span>目标域名</span>
          <input value={domain} onChange={(event) => setDomain(event.target.value)} />
        </label>
        <label className="filter-item">
          <span>请求编号</span>
          <input value={requestCode} onChange={(event) => setRequestCode(event.target.value)} />
        </label>
        <label className="filter-item">
          <span>目标URL</span>
          <input value={targetUrl} onChange={(event) => setTargetUrl(event.target.value)} />
        </label>
        <button className="btn primary" type="submit">写入新增请求</button>
        <button className="btn ghost" type="button" onClick={() => void reload()}>刷新</button>
      </form>

      <h3 className="section-title">在岗清单</h3>
      <table className="data-table">
        <thead>
          <tr>{rosterColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {roster.map((row) => (
            <tr key={String(row.id)}>
              {rosterColumns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {roster.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={rosterColumns.length}>该域名暂无在岗节点，先处理心跳或同步可调度结果</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">请求分流记录（只追加）</h3>
      <table className="data-table">
        <thead>
          <tr>{assignmentColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {assignments.map((row) => (
            <tr key={String(row.id)}>
              {assignmentColumns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {assignments.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={assignmentColumns.length}>暂无请求分流记录</td>
            </tr>
          )}
        </tbody>
      </table>

      <h3 className="section-title">同步记录</h3>
      <table className="data-table">
        <thead>
          <tr>{syncColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {syncs.map((row) => (
            <tr key={String(row.id)}>
              {syncColumns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {syncs.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={syncColumns.length}>暂无同步记录</td>
            </tr>
          )}
        </tbody>
      </table>

      {checks.length > 0 && (
        <>
          <h3 className="section-title">一致性校验</h3>
          <table className="data-table">
            <thead>
              <tr><th>校验项</th><th>结果</th><th>详情</th></tr>
            </thead>
            <tbody>
              {checks.map((item) => (
                <tr key={item['校验项']}>
                  <td>{item['校验项']}</td>
                  <td>{item['通过'] ? '通过' : '未通过'}</td>
                  <td>{item['详情'] || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <footer className="page-foot">
        <span>在岗 {roster.filter((row) => row['状态'] === '在岗').length} 条，分流记录 {assignments.length} 条</span>
        {message && <span>{message}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

function formatCell(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}
