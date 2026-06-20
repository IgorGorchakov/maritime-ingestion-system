import { cn } from '@/lib/utils'

export function Separator({ className, orientation = 'horizontal', ...props }) {
  return (
    <div
      role="separator"
      className={cn(
        'shrink-0 bg-slate-800',
        orientation === 'horizontal' ? 'h-px w-full' : 'h-full w-px',
        className
      )}
      {...props}
    />
  )
}
