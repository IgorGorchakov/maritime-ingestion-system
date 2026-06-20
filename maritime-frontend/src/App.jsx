import { useState, useEffect, useRef } from 'react'
import { useFleet } from '@/hooks/useFleet'
import AppHeader from '@/components/AppHeader'
import VesselMap from '@/components/VesselMap'
import VesselPanel from '@/components/VesselPanel'

/** Maximum positions kept per vessel trail. */
const MAX_TRACK_POINTS = 60

export default function App() {
  const fleet = useFleet()
  const [selectedMmsi, setSelectedMmsi] = useState(null)
  const [simRunning, setSimRunning]     = useState(false)

  // Accumulated position history per vessel: mmsi → [{lat, lon}]
  // Stored in a ref so accumulation doesn't cause re-renders on every poll.
  const tracksRef = useRef(new Map())
  const [tracks, setTracks] = useState(new Map())

  useEffect(() => {
    let changed = false
    fleet.forEach(({ mmsi, data }) => {
      if (!data?.vesselEvent) return
      const { latitude: lat, longitude: lon } = data.vesselEvent
      const prev = tracksRef.current.get(mmsi) ?? []
      const last = prev[prev.length - 1]
      // Only append if position actually changed
      if (last && last.lat === lat && last.lon === lon) return
      const next = [...prev, { lat, lon }].slice(-MAX_TRACK_POINTS)
      tracksRef.current.set(mmsi, next)
      changed = true
    })
    if (changed) setTracks(new Map(tracksRef.current))
  }, [fleet])

  const selectedVessel = fleet.find(v => v.mmsi === selectedMmsi) ?? null

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-100">
      <AppHeader simRunning={simRunning} setSimRunning={setSimRunning} />

      <div className="flex flex-1 overflow-hidden">
        <div className={selectedMmsi ? 'flex-1' : 'w-full'}>
          <VesselMap
            fleet={fleet}
            tracks={tracks}
            selectedMmsi={selectedMmsi}
            onSelectVessel={setSelectedMmsi}
          />
        </div>

        {selectedMmsi && (
          <div className="w-96 border-l border-slate-800 overflow-y-auto shrink-0">
            <VesselPanel
              vessel={selectedVessel}
              onClose={() => setSelectedMmsi(null)}
            />
          </div>
        )}
      </div>
    </div>
  )
}
