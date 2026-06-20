import { Marker, Tooltip } from 'react-leaflet'
import L from 'leaflet'

function markerColor(riskLevel, darkVessel) {
  if (darkVessel)              return '#111827' // dark vessel — near-black
  if (riskLevel === 'HIGH')    return '#ef4444'
  if (riskLevel === 'MEDIUM')  return '#f59e0b'
  return '#22c55e' // LOW
}

function makeIcon(color, heading, selected) {
  const size = selected ? 22 : 16
  // Triangle pointing north, rotated by vessel heading
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 16 16">
      <g transform="rotate(${heading ?? 0}, 8, 8)">
        <polygon
          points="8,1 14,14 8,11 2,14"
          fill="${color}"
          stroke="white"
          stroke-width="${selected ? 2 : 1.5}"
          opacity="${selected ? 1 : 0.9}"
        />
      </g>
    </svg>`

  return L.divIcon({
    html: svg,
    className: '',           // suppress Leaflet's default white-box background
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  })
}

export default function VesselMarker({ vessel, selected, onClick }) {
  const { data, mmsi, label } = vessel
  const { vesselEvent, riskLevel, loitering, darkVessel, speedAnomaly } = data

  const position = [vesselEvent.latitude, vesselEvent.longitude]
  const color    = markerColor(riskLevel, darkVessel)
  const icon     = makeIcon(color, vesselEvent.heading, selected)

  const activeFlags = [
    loitering    && 'Loitering',
    darkVessel   && 'Dark Vessel',
    speedAnomaly && 'Speed Anomaly',
  ].filter(Boolean)

  return (
    <Marker position={position} icon={icon} eventHandlers={{ click: onClick }}>
      <Tooltip direction="top" offset={[0, -10]} opacity={0.97}>
        <div style={{ fontSize: 12, lineHeight: 1.5 }}>
          <div style={{ fontWeight: 600 }}>{label}</div>
          <div style={{ color: '#64748b' }}>MMSI {mmsi}</div>
          <div>{vesselEvent.speed.toFixed(1)} kn &middot; {riskLevel}</div>
          {activeFlags.length > 0 && (
            <div style={{ color: '#d97706' }}>{activeFlags.join(' · ')}</div>
          )}
        </div>
      </Tooltip>
    </Marker>
  )
}
