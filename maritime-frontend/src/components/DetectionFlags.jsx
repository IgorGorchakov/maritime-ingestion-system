import { Badge } from '@/components/ui/badge'

const FLAGS = [
  { key: 'loitering',    label: 'Loitering',    style: 'bg-amber-900/40 text-amber-300 border-amber-600' },
  { key: 'darkVessel',   label: 'Dark Vessel',   style: 'bg-purple-900/40 text-purple-300 border-purple-600' },
  { key: 'speedAnomaly', label: 'Speed Anomaly', style: 'bg-orange-900/40 text-orange-300 border-orange-600' },
]

export default function DetectionFlags({ data }) {
  const active = FLAGS.filter(f => data[f.key])
  if (active.length === 0) return null

  return (
    <div className="flex flex-wrap gap-1.5">
      {active.map(f => (
        <Badge key={f.key} variant="outline" className={f.style}>
          {f.label}
        </Badge>
      ))}
    </div>
  )
}
