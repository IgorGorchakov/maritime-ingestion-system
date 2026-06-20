import { useState } from 'react'
import { useFleet } from '@/hooks/useFleet'
import AppHeader from '@/components/AppHeader'
import VesselMap from '@/components/VesselMap'
import VesselPanel from '@/components/VesselPanel'

export default function App() {
  const fleet = useFleet()
  const [selectedMmsi, setSelectedMmsi] = useState(null)
  const [simRunning, setSimRunning] = useState(false)

  const selectedVessel = fleet.find(v => v.mmsi === selectedMmsi) ?? null

  return (
    <div className="flex flex-col h-screen bg-slate-950 text-slate-100">
      <AppHeader simRunning={simRunning} setSimRunning={setSimRunning} />

      <div className="flex flex-1 overflow-hidden">
        <div className={selectedMmsi ? 'flex-1' : 'w-full'}>
          <VesselMap
            fleet={fleet}
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
