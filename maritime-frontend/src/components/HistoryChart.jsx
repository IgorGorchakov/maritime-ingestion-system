import { useQuery } from '@tanstack/react-query'
import {
  ComposedChart, Bar, Line,
  XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { fetchHistory } from '@/api/maritime'

function shortDate(iso) {
  const [, m, d] = iso.split('-')
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  return `${months[+m - 1]} ${+d}`
}

export default function HistoryChart({ mmsi }) {
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['history', mmsi],
    queryFn: () => fetchHistory(mmsi),
    staleTime: 5 * 60 * 1000, // history only changes when Spark runs (daily)
    retry: false,
  })

  if (isLoading) {
    return <p className="text-xs text-slate-500 py-4 text-center">Loading…</p>
  }

  if (history.length === 0) {
    return (
      <p className="text-xs text-slate-500 py-4 text-center">
        No batch data yet — Spark jobs run once per day.
      </p>
    )
  }

  // API returns newest-first; reverse so X axis runs oldest → newest
  const rows = [...history].reverse().map(r => ({
    ...r,
    dateLabel: shortDate(r.date),
  }))

  return (
    <ResponsiveContainer width="100%" height={200}>
      <ComposedChart data={rows} margin={{ top: 4, right: 12, left: -16, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#334155" />

        <XAxis
          dataKey="dateLabel"
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          interval="preserveStartEnd"
        />

        {/* Left axis — event count */}
        <YAxis
          yAxisId="count"
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          width={38}
        />

        {/* Right axis — risk scores 0–100 */}
        <YAxis
          yAxisId="risk"
          orientation="right"
          domain={[0, 100]}
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          width={30}
        />

        <Tooltip
          contentStyle={{
            backgroundColor: '#0f172a',
            border: '1px solid #1e293b',
            borderRadius: 6,
            fontSize: 11,
          }}
          labelStyle={{ color: '#94a3b8' }}
          formatter={(value, name) => {
            const labels = { eventCount: 'Events', avgRiskScore: 'Avg Risk', p95Risk: 'p95 Risk' }
            return [typeof value === 'number' ? value.toFixed(1) : value, labels[name] ?? name]
          }}
        />

        <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} iconSize={10} />

        <Bar
          yAxisId="count"
          dataKey="eventCount"
          name="Events"
          fill="#1e40af"
          opacity={0.7}
          radius={[2, 2, 0, 0]}
        />
        <Line
          yAxisId="risk"
          type="monotone"
          dataKey="avgRiskScore"
          name="Avg Risk"
          stroke="#f59e0b"
          strokeWidth={1.5}
          dot={false}
        />
        <Line
          yAxisId="risk"
          type="monotone"
          dataKey="p95Risk"
          name="p95 Risk"
          stroke="#ef4444"
          strokeWidth={1.5}
          dot={false}
          strokeDasharray="4 2"
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
