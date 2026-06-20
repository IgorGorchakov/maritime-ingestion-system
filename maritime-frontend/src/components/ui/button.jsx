import { cva } from 'class-variance-authority'
import { cn } from '@/lib/utils'

const buttonVariants = cva(
  'inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50',
  {
    variants: {
      variant: {
        default: 'bg-slate-700 text-slate-100 hover:bg-slate-600',
        outline: 'border border-slate-600 bg-transparent hover:bg-slate-800',
        ghost:   'hover:bg-slate-800 hover:text-slate-100',
      },
      size: {
        default: 'h-9 px-4 py-2',
        sm:      'h-8 px-3 text-xs',
        icon:    'h-8 w-8',
      },
    },
    defaultVariants: { variant: 'default', size: 'default' },
  }
)

export function Button({ className, variant, size, ...props }) {
  return <button className={cn(buttonVariants({ variant, size }), className)} {...props} />
}
