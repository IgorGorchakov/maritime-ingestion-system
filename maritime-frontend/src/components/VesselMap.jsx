import { MapContainer, TileLayer } from 'react-leaflet'
import VesselMarker from './VesselMarker'

// Gulf of Mexico — matches the simulator's coordinate space
const CENTER = [30.5, -88.0]
const ZOOM = 7

export default function VesselMap({ fleet, selectedMmsi, onSelectVessel }) {
  return (
    <div style={{ height: '100%', width: '100%' }}>
      <MapContainer
        center={CENTER}
        zoom={ZOOM}
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        />

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
