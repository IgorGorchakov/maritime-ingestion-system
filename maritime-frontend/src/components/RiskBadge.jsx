import { Badge } from '@/components/ui/badge'

const STYLES = {
  HIGH:   'bg-red-900/50 text-red-300 border-red-700',
  MEDIUM: 'bg-amber-900/50 text-amber-300 border-amber-700',
  LOW:    'bg-emerald-900/50 text-emerald-300 border-emerald-700',
}

export default function RiskBadge({ level }) {
  return (
    <Badge variant="outline" className={STYLES[level] ?? STYLES.LOW}>
      {level}
    </Badge>
  )
}
