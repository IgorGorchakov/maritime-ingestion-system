import { MapContainer, TileLayer, Polyline } from 'react-leaflet'
import VesselMarker from './VesselMarker'

const CENTER = [30.5, -88.0]
const ZOOM   = 7

/** Trail colour matches the marker colour scheme. */
function trailColor(vessel) {
  if (vessel.data?.darkVessel) return '#374151'   // dark vessel — dark grey trail
  const level = vessel.data?.riskLevel
  if (level === 'HIGH')   return '#ef4444'
  if (level === 'MEDIUM') return '#f59e0b'
  return '#22c55e'
}

export default function VesselMap({ fleet, tracks, selectedMmsi, onSelectVessel }) {
  return (
    <div style={{ height: '100%', width: '100%' }}>
      <MapContainer center={CENTER} zoom={ZOOM} style={{ height: '100%', width: '100%' }}>
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        />

        {/* Trajectory trails — rendered before markers so markers sit on top */}
        {fleet.map(vessel => {
          const points = tracks?.get(vessel.mmsi)
          if (!points || points.length < 2) return null
          const isSelected = vessel.mmsi === selectedMmsi
          return (
            <Polyline
              key={`trail-${vessel.mmsi}`}
              positions={points.map(p => [p.lat, p.lon])}
              pathOptions={{
                color:   trailColor(vessel),
                weight:  isSelected ? 3 : 1.5,
                opacity: isSelected ? 0.85 : 0.45,
                dashArray: vessel.data?.darkVessel ? '4 6' : null,
              }}
            />
          )
        })}

        {/* Vessel markers */}
        {fleet.map(vessel =>
          vessel.data ? (
            <VesselMarker
              key={vessel.mmsi}
              vessel={vessel}
              selected={vessel.mmsi === selectedMmsi}
              onClick={() => onSelectVessel(vessel.mmsi)}
            />
          ) : null
        )}
      </MapContainer>
    </div>
  )
}
