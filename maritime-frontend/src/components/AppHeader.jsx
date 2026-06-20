import { startSim, stopSim } from '@/api/maritime'
import { Button } from '@/components/ui/button'

export default function AppHeader({ simRunning, setSimRunning }) {
  async function handleStart() {
    await startSim()
    setSimRunning(true)
  }

  async function handleStop() {
    await stopSim()
    setSimRunning(false)
  }

  return (
    <header className="flex items-center justify-between px-4 py-2 bg-slate-900 border-b border-slate-800 shrink-0">
      <div className="flex items-center gap-3">
        <span className="text-lg font-semibold tracking-wide text-slate-100">
          Maritime Intelligence
        </span>

        {simRunning && (
          <span className="flex items-center gap-1.5 text-xs text-emerald-400">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
            </span>
            LIVE
          </span>
        )}
      </div>

      <div className="flex gap-2">
        <Button
          size="sm"
          variant="outline"
          onClick={handleStart}
          disabled={simRunning}
          className="border-emerald-700 text-emerald-400 hover:bg-emerald-950 disabled:opacity-40"
        >
          Start Simulation
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={handleStop}
          disabled={!simRunning}
          className="border-slate-600 text-slate-300 hover:bg-slate-800 disabled:opacity-40"
        >
          Stop
        </Button>
      </div>
    </header>
  )
}
