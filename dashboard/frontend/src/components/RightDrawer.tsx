import * as Dialog from '@radix-ui/react-dialog'

interface RightDrawerProps {
  open: boolean
  onClose: () => void
  title: string
  children: React.ReactNode
}

export function RightDrawer({ open, onClose, title, children }: RightDrawerProps) {
  return (
    <Dialog.Root open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/50 z-40" />
        <Dialog.Content className="fixed right-0 top-0 bottom-0 w-[480px] bg-bg-card border-l border-border-default z-50 overflow-y-auto shadow-modal">
          <div className="flex items-center justify-between p-4 border-b border-border-default">
            <Dialog.Title className="font-heading text-lg text-text-primary">
              {title}
            </Dialog.Title>
            <Dialog.Close asChild>
              <button className="text-text-muted hover:text-text-primary text-xl leading-none">
                ×
              </button>
            </Dialog.Close>
          </div>
          <div className="p-4">{children}</div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
