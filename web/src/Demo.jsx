import { useState } from 'react'

// Same FNV-1a as broker.rs::partition_for
function partitionFor(key, n) {
  let h = 0xcbf29ce484222325n
  for (const b of new TextEncoder().encode(key)) h = BigInt.asUintN(64, (h ^ BigInt(b)) * 0x100000001b3n)
  return Number(h % BigInt(n))
}
// Same rule as broker.rs::join: partition p belongs to member p % count
const assignment = (members, n) => Object.fromEntries([...members].sort().map((m, i, all) => [m, Array.from({ length: n }, (_, p) => p).filter((p) => p % all.length === i)]))
const COLORS = ['#c2410c', '#2563eb', '#15803d', '#9333ea', '#b45309', '#0e7490']
const KEYS = ['alice', 'bob', 'carol', 'dave', 'erin']

export default function Demo() {
  const [n, setN] = useState(3)
  const [logs, setLogs] = useState(() => Array.from({ length: 3 }, () => []))
  const [committed, setCommitted] = useState([0, 0, 0])
  const [members, setMembers] = useState(['consumer-1'])
  const [key, setKey] = useState('alice')
  const [rr, setRr] = useState(0)
  const [events, setEvents] = useState([])
  const log = (s) => setEvents((e) => [s, ...e].slice(0, 12))
  const assign = assignment(members, n)
  const owner = (p) => Object.keys(assign).find((m) => assign[m].includes(p))

  function reset(parts) {
    setN(parts); setLogs(Array.from({ length: parts }, () => [])); setCommitted(Array(parts).fill(0)); setEvents([])
  }
  function produce(k) {
    const p = k ? partitionFor(k, n) : rr % n
    if (!k) setRr(rr + 1)
    setLogs((ls) => ls.map((l, i) => (i === p ? [...l, { key: k, value: `msg ${l.length}` }] : l)))
    log(`produce key=${k || '∅'} → partition ${p} @ offset ${logs[p].length}`)
  }
  function burst() { for (let i = 0; i < 8; i++) setTimeout(() => produce(KEYS[Math.floor(Math.random() * KEYS.length)]), i * 60) }
  function poll(m) {
    const parts = assign[m] || []
    let got = 0
    const next = [...committed]
    for (const p of parts) { got += logs[p].length - next[p]; next[p] = logs[p].length }
    setCommitted(next)
    log(`${m} polled ${got} record(s) from [${parts.join(', ')}] and committed`)
  }
  function addMember() {
    const id = `consumer-${members.length + 1}`
    setMembers([...members, id]); log(`${id} joined → rebalance`)
  }
  function removeMember(m) { setMembers(members.filter((x) => x !== m)); log(`${m} left → rebalance`) }
  const totalLag = logs.reduce((s, l, p) => s + l.length - committed[p], 0)

  return (
    <div>
      <div className="row">
        <label>Partitions <select value={n} onChange={(e) => reset(Number(e.target.value))}>{[1, 2, 3, 4, 6].map((x) => <option key={x}>{x}</option>)}</select></label>
        <select value={key} onChange={(e) => setKey(e.target.value)} aria-label="message key">
          {KEYS.map((k) => <option key={k}>{k}</option>)}<option value="">(no key)</option>
        </select>
        <button className="primary" onClick={() => produce(key)}>Produce</button>
        <button onClick={burst}>Produce 8 random</button>
        <span className="pill">total lag: {totalLag}</span>
      </div>
      <div style={{ marginTop: 12 }}>
        {logs.map((l, p) => {
          const o = owner(p)
          const color = COLORS[members.indexOf(o) % COLORS.length]
          return (
            <div key={p} style={{ display: 'flex', alignItems: 'center', gap: 8, margin: '6px 0' }}>
              <div style={{ width: 120, fontSize: 13 }}><b>P{p}</b> <span style={{ color }}>{o || 'unassigned'}</span></div>
              <div style={{ display: 'flex', gap: 3, flexWrap: 'wrap', flex: 1 }}>
                {l.length === 0 && <span className="muted" style={{ fontSize: 13 }}>empty</span>}
                {l.map((m, i) => (
                  <div key={i} title={`offset ${i} key=${m.key || '∅'}`} style={{ minWidth: 34, padding: '2px 4px', borderRadius: 4, fontSize: 11, fontFamily: 'var(--mono)', textAlign: 'center', background: i < committed[p] ? 'var(--surface-2)' : 'var(--accent-soft)', color: i < committed[p] ? 'var(--muted)' : 'var(--text)', borderLeft: i === committed[p] ? '2px solid var(--accent)' : '2px solid transparent' }}>
                    {i}<br />{m.key ? m.key.slice(0, 3) : '∅'}
                  </div>
                ))}
              </div>
              <span className="pill">lag {l.length - committed[p]}</span>
            </div>
          )
        })}
      </div>
      <h3 style={{ marginTop: 16 }}>Consumer group “billing”</h3>
      <div className="row">
        {members.map((m, i) => (
          <span key={m} className="card" style={{ padding: '6px 10px', borderColor: COLORS[i % COLORS.length] }}>
            <b style={{ color: COLORS[i % COLORS.length] }}>{m}</b> <span className="muted">[{(assign[m] || []).join(', ') || 'idle'}]</span>{' '}
            <button onClick={() => poll(m)}>Poll</button>{' '}
            {members.length > 1 && <button onClick={() => removeMember(m)} aria-label={`remove ${m}`}>✕</button>}
          </span>
        ))}
        {members.length < 6 && <button onClick={addMember}>+ Add consumer</button>}
      </div>
      <div className="log">{events.join('\n') || 'Produce something to begin.'}</div>
    </div>
  )
}
