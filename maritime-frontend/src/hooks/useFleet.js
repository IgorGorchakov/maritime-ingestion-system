import { useQueries } from '@tanstack/react-query'
import { FLEET, fetchVessel } from '@/api/maritime'

/**
 * Polls all 4 vessels at the same 2 s cadence as the backend simulator.
 * Returns an array of { mmsi, label, data, isLoading } objects.
 * data is the EnrichedVesselEvent shape, or null if the vessel is offline.
 */
export function useFleet() {
  const results = useQueries({
    queries: FLEET.map(({ mmsi }) => ({
      queryKey: ['vessel', mmsi],
      queryFn: () => fetchVessel(mmsi),
      refetchInterval: 2000,
      staleTime: 0,
      retry: false, // dark vessel returns 404 after going silent — don't hammer it
    })),
  })

  return FLEET.map(({ mmsi, label }, i) => ({
    mmsi,
    label,
    data: results[i].data ?? null,
    isLoading: results[i].isLoading,
  }))
}
