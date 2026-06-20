// Fleet mirrors AisSimulatorController.java (all 9 vessels)
export const FLEET = [
  { mmsi: '123456789', label: 'Normal Transit' },
  { mmsi: '234567890', label: 'Loiterer' },
  { mmsi: '345678901', label: 'Dark Vessel' },
  { mmsi: '456789012', label: 'Speed Anomaly' },
  { mmsi: '111111111', label: 'Tanker Alpha' },
  { mmsi: '222222222', label: 'Tanker Bravo' },
  { mmsi: '333333333', label: 'Cargo Alpha' },
  { mmsi: '444444444', label: 'Cargo Bravo' },
  { mmsi: '555555555', label: 'Fishing Vessel' },
]

/**
 * GET /api/v1/intelligence/{mmsi}
 * Returns the latest EnrichedVesselEvent or null if not found.
 */
export async function fetchVessel(mmsi) {
  const res = await fetch(`/api/v1/intelligence/${mmsi}`)
  if (!res.ok) return null
  return res.json()
}

/**
 * GET /api/v1/intelligence/{mmsi}/history
 * Returns up to 90 daily summary rows, newest first.
 * Empty array if Spark jobs haven't run yet.
 */
export async function fetchHistory(mmsi) {
  const res = await fetch(`/api/v1/intelligence/${mmsi}/history`)
  if (!res.ok) return []
  return res.json()
}

/**
 * POST /api/v1/simulate/start  (maritime-ingestion :8081)
 * Starts the 4-vessel AIS simulator emitting events every 2 seconds.
 */
export async function startSim() {
  const res = await fetch('/api/v1/simulate/start', { method: 'POST' })
  return res.text()
}

/**
 * POST /api/v1/simulate/stop  (maritime-ingestion :8081)
 * Stops the simulator. Existing vessel data remains in the hot store.
 */
export async function stopSim() {
  const res = await fetch('/api/v1/simulate/stop', { method: 'POST' })
  return res.text()
}
