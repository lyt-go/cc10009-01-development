import { useCallback, useEffect, useState } from 'react'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/schedulable'

const columns = [
  '节点编号',
  '主机地址',
  '并发槽位数',
  '空闲槽位',
  '占用槽位',
  '租约版本',
  '租约到期时间',
  '判定序号',
  '槽位快照摘要',
]

const batchColumns = ['版本', '批次数', '水位时间', '触发', '条目数', '内容摘要', '状态']

const conflictColumns = ['节点编号', '前一版本', '当前版本', '采用判定序号', '采用结果']

type VerifyResult = {
  ok: boolean
  批次版本?: number
  内容摘要?: string
  校验项?: { 校验项: string; 通过: boolean; 详情: string }[]
}

export default function SchedulableView() {
  const [rows, setRows] = useState<Row[]>([])
  const [meta, setMeta] = useState<Record<string, unknown>>({})
  const [batches, setBatches] = useState<Record<string, unknown>[]>([])
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null)
  const [keyword, setKeyword] = useState('')
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const reload = useCallback(async () => {
    try {
      const query = keyword ? `?keyword=${encodeURIComponent(keyword)}` : ''
      const [itemsResponse, metaResponse, batchResponse] = await Promise.all([
        request(`${ENDPOINT}${query}`),
        request(`${ENDPOINT}/meta`),
        request(`${ENDPOINT}/batches`),
      ])
      if (!itemsResponse.ok || !metaResponse.ok || !batchResponse.ok) {
        throw new Error('可调度结果读取失败')
      }
      const itemsPayload = await itemsResponse.json()
      setRows(itemsPayload.items ?? [])
      setMeta(await metaResponse.json())
      const batchPayload = await batchResponse.json()
      setBatches(batchPayload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '可调度节点结果读取失败')
    }
  }, [keyword])

  useEffect(() => {
    void reload()
  }, [reload])

  async function publish() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/publish`, {
        method: 'POST',
        body: JSON.stringify({ trigger: '界面手动发布' }),
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload?.detail ?? '发布失败')
      setMessage(String(payload.message ?? '已发布'))
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '发布失败')
    }
  }

  async function verify() {
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/verify`)
      if (!response.ok) throw new Error('完整性校验接口返回异常')
      const payload = (await response.json()) as VerifyResult
      setVerifyResult(payload)
      setMessage(payload.ok ? '完整性校验通过' : '完整性校验未通过，请查看校验项')
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '完整性校验失败')
    }
  }

  const conflicts = (meta['边界冲突'] ?? []) as Record<string, unknown>[]
  const mergedVersions = (meta['合并版本'] ?? []) as number[]

  return (
    <section className="page" data-module="schedulable">
      <header className="page-head">
        <div>
          <h2>可调度节点结果</h2>
          <p className="page-desc">
            按租约与槽位快照生成可调度节点结果；最近两个批次合并展示，批次边界冲突单独列出并给出裁决。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => void publish()}>发布新批次</button>
          <button className="btn" type="button" onClick={() => void verify()}>校验完整性</button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">可调度节点数</span>
          <strong className="stat-value">{String(meta['可调度节点数'] ?? 0)}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">当前批次版本</span>
          <strong className="stat-value">{String(meta['当前版本'] ?? '—')}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">合并批次</span>
          <strong className="stat-value">{mergedVersions.join(' / ') || '—'}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">边界冲突</span>
          <strong className="stat-value">{conflicts.length}</strong>
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
          <input value={keyword} placeholder="按节点编号检索" onChange={(event) => setKeyword(event.target.value)} />
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setKeyword('')}>重置条件</button>
      </form>

      <h3 className="section-title">合并结果（{String(meta['批次状态'] ?? '—')}）</h3>
      <table className="data-table">
        <thead>
          <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row['节点编号'])}>
              {columns.map((column) => <td key={column}>{formatCell(row[column])}</td>)}
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length}>当前没有可调度节点，需先处理心跳或槽位快照</td>
            </tr>
          )}
        </tbody>
      </table>

      {conflicts.length > 0 && (
        <>
          <h3 className="section-title">批次边界冲突</h3>
          <table className="data-table">
            <thead>
              <tr>{conflictColumns.map((column) => <th key={column}>{column}</th>)}</tr>
            </thead>
            <tbody>
              {conflicts.map((conflict) => {
                const previous = (conflict['前一版本'] ?? {}) as Record<string, unknown>
                const current = (conflict['当前版本'] ?? {}) as Record<string, unknown>
                return (
                  <tr key={String(conflict['节点编号'])}>
                    <td>{String(conflict['节点编号'])}</td>
                    <td>v{String(previous['版本'])}：{previous['可调度'] ? '可调度' : '不可调度'}</td>
                    <td>v{String(current['版本'])}：{current['可调度'] ? '可调度' : '不可调度'}</td>
                    <td>{formatCell(conflict['采用判定序号'] as string | number | null)}</td>
                    <td>{String(conflict['采用结果'] ?? '—')}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </>
      )}

      <h3 className="section-title">保留的批次</h3>
      <table className="data-table">
        <thead>
          <tr>{batchColumns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {batches.map((batch) => (
            <tr key={String(batch['版本'])}>
              {batchColumns.map((column) => <td key={column}>{formatCell(batch[column] as string | number | null)}</td>)}
            </tr>
          ))}
          {batches.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={batchColumns.length}>暂无批次</td>
            </tr>
          )}
        </tbody>
      </table>

      {verifyResult && (
        <>
          <h3 className="section-title">完整性校验</h3>
          <table className="data-table">
            <thead>
              <tr><th>校验项</th><th>结果</th><th>详情</th></tr>
            </thead>
            <tbody>
              {(verifyResult['校验项'] ?? []).map((item) => (
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
        <span>当前版本 {String(meta['当前版本'] ?? '—')}，内容摘要 {String(meta['内容摘要'] ?? '—')}</span>
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
