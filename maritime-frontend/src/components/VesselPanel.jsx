import { Separator } from '@/components/ui/separator'
import { Button } from '@/components/ui/button'
import RiskBadge from './RiskBadge'
import DetectionFlags from './DetectionFlags'
import HistoryChart from './HistoryChart'

function fmtTime(ms) {
  return new Date(ms).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function Row({ label, value }) {
  return (
    <>
      <span className="text-sm text-slate-400">{label}</span>
      <span className="text-sm font-mono text-right text-slate-200">{value}</span>
    </>
  )
}

export default function VesselPanel({ vessel, onClose }) {
  if (!vessel) return null

  const { mmsi, label, data } = vessel

  return (
    <div className="h-full bg-slate-900 flex flex-col">
      {/* Header */}
      <div className="flex items-start justify-between p-4 shrink-0">
        <div>
          <p className="text-xs text-slate-500 uppercase tracking-widest mb-0.5">Vessel</p>
          <h2 className="text-base font-semibold text-slate-100">{label}</h2>
          <p className="text-xs text-slate-500 font-mono">{mmsi}</p>
        </div>
        <Button
          size="icon"
          variant="ghost"
          onClick={onClose}
          className="text-slate-500 hover:text-slate-200 -mr-1 -mt-1"
        >
          ✕
        </Button>
      </div>

      <Separator />

      {data ? (
        <div className="p-4 space-y-4 overflow-y-auto flex-1">
          {/* Risk level */}
          <div className="flex items-center justify-between">
            <span className="text-sm text-slate-400">Risk Level</span>
            <RiskBadge level={data.riskLevel} />
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm text-slate-400">Risk Score</span>
            <span className="text-sm font-mono text-slate-200">{data.riskScore.toFixed(1)}</span>
          </div>

          <Separator />

          {/* Movement */}
          <div className="grid grid-cols-2 gap-x-4 gap-y-2">
            <Row label="Speed"          value={`${data.vesselEvent.speed.toFixed(1)} kn`} />
            <Row label="Heading"        value={`${data.vesselEvent.heading.toFixed(0)}°`} />
            <Row label="Distance to port" value={`${data.distanceToPort.toFixed(1)} nm`} />
            <Row label="Last seen"      value={fmtTime(data.vesselEvent.timestamp)} />
          </div>

          {/* Zone (only when in a restricted zone) */}
          {data.inRestrictedZone && (
            <>
              <Separator />
              <div className="text-sm space-y-0.5">
                <p className="text-red-400 font-medium">In Restricted Zone</p>
                {data.zoneName && (
                  <p className="text-slate-400">{data.zoneName} ({data.zoneType})</p>
                )}
              </div>
            </>
          )}

          {/* Detection flags (only shown when at least one is true) */}
          <DetectionFlags data={data} />

          <Separator />

          {/* History */}
          <div>
            <p className="text-xs text-slate-500 uppercase tracking-widest mb-3">90-day History</p>
            <HistoryChart mmsi={mmsi} />
          </div>
        </div>
      ) : (
        <div className="flex flex-1 items-center justify-center">
          <p className="text-sm text-slate-500">No data — vessel offline</p>
        </div>
      )}
    </div>
  )
}
