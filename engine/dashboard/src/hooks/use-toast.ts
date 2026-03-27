/**
 * @module Global toast notification store inspired by react-hot-toast. Provides a module-level reducer, a `toast()` factory with auto-dismiss support, and a `useToast` hook that subscribes React components to toast state changes.
 */
"use client"

// Inspired by react-hot-toast library
import * as React from "react"

import type {
  ToastActionElement,
  ToastProps,
} from "@/components/ui/toast"

const TOAST_LIMIT = 1
const TOAST_REMOVE_DELAY = 1000
const DEFAULT_TOAST_DURATION = 5000

type ToasterToast = ToastProps & {
  id: string
  title?: React.ReactNode
  description?: React.ReactNode
  action?: ToastActionElement
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars -- used only as typeof for Action type union
const actionTypes = {
  ADD_TOAST: "ADD_TOAST",
  UPDATE_TOAST: "UPDATE_TOAST",
  DISMISS_TOAST: "DISMISS_TOAST",
  REMOVE_TOAST: "REMOVE_TOAST",
} as const

let count = 0

function genId() {
  count = (count + 1) % Number.MAX_SAFE_INTEGER
  return count.toString()
}

type ActionType = typeof actionTypes

/**
 * Discriminated union of toast state actions.
 * Each variant carries the minimal payload needed to add, update, dismiss, or remove a toast from the store.
 */
type Action =
  | {
      type: ActionType["ADD_TOAST"]
      toast: ToasterToast
    }
  | {
      type: ActionType["UPDATE_TOAST"]
      toast: Partial<ToasterToast>
    }
  | {
      type: ActionType["DISMISS_TOAST"]
      toastId?: ToasterToast["id"]
    }
  | {
      type: ActionType["REMOVE_TOAST"]
      toastId?: ToasterToast["id"]
    }

interface State {
  toasts: ToasterToast[]
}

const toastTimeouts = new Map<string, ReturnType<typeof setTimeout>>()

const addToRemoveQueue = (toastId: string) => {
  if (toastTimeouts.has(toastId)) {
    return
  }

  const timeout = setTimeout(() => {
    toastTimeouts.delete(toastId)
    dispatch({
      type: "REMOVE_TOAST",
      toastId: toastId,
    })
  }, TOAST_REMOVE_DELAY)

  toastTimeouts.set(toastId, timeout)
}

/**
 * Pure reducer that manages the toast list.
 * Enforces `TOAST_LIMIT` on additions, merges partial updates, and schedules deferred removal on dismiss.
 * @param state - Current toast state.
 * @param action - Dispatched action.
 * @returns Next state with the updated toasts array.
 */
export const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case "ADD_TOAST":
      return {
        ...state,
        toasts: [action.toast, ...state.toasts].slice(0, TOAST_LIMIT),
      }

    case "UPDATE_TOAST":
      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === action.toast.id ? { ...t, ...action.toast } : t
        ),
      }

    case "DISMISS_TOAST": {
      const { toastId } = action

      // ! Side effects ! - This could be extracted into a dismissToast() action,
      // but I'll keep it here for simplicity
      if (toastId) {
        addToRemoveQueue(toastId)
      } else {
        state.toasts.forEach((toast) => {
          addToRemoveQueue(toast.id)
        })
      }

      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === toastId || toastId === undefined
            ? {
                ...t,
                open: false,
              }
            : t
        ),
      }
    }
    case "REMOVE_TOAST":
      if (action.toastId === undefined) {
        return {
          ...state,
          toasts: [],
        }
      }
      return {
        ...state,
        toasts: state.toasts.filter((t) => t.id !== action.toastId),
      }
  }
}

const listeners: Array<(state: State) => void> = []

let memoryState: State = { toasts: [] }

function dispatch(action: Action) {
  memoryState = reducer(memoryState, action)
  listeners.forEach((listener) => {
    listener(memoryState)
  })
}

type Toast = Omit<ToasterToast, "id"> & {
  /** Auto-dismiss delay in ms. 0 = stay visible until manually dismissed. Default: 5000 */
  duration?: number
}

const autoDismissTimers = new Map<string, ReturnType<typeof setTimeout>>()

/**
 * Creates and dispatches a new toast notification.
 * Returns handles to programmatically update or dismiss it. Auto-dismisses after `duration` ms (default 5 000); pass `0` to keep the toast visible until manually dismissed.
 * @param props - Toast content and optional duration override.
 * @returns Object with the generated `id`, a `dismiss` callback, and an `update` callback.
 */
function toast({ duration, ...props }: Toast) {
  const id = genId()

  const update = (props: ToasterToast) =>
    dispatch({
      type: "UPDATE_TOAST",
      toast: { ...props, id },
    })
  const dismiss = () => {
    // Clear any pending auto-dismiss timer
    const timer = autoDismissTimers.get(id)
    if (timer) {
      clearTimeout(timer)
      autoDismissTimers.delete(id)
    }
    dispatch({ type: "DISMISS_TOAST", toastId: id })
  }

  dispatch({
    type: "ADD_TOAST",
    toast: {
      ...props,
      id,
      open: true,
      onOpenChange: (open) => {
        if (!open) dismiss()
      },
    },
  })

  // Auto-dismiss after duration (default 5s, 0 = persistent)
  const ms = duration ?? DEFAULT_TOAST_DURATION
  if (ms > 0) {
    autoDismissTimers.set(id, setTimeout(dismiss, ms))
  }

  return {
    id: id,
    dismiss,
    update,
  }
}

/**
 * Subscribes to the global toast store and returns the current toasts plus helper methods.
 * @returns The current toast list, the `toast` creation function, and a `dismiss` function.
 */
function useToast() {
  const [state, setState] = React.useState<State>(memoryState)

  React.useEffect(() => {
    listeners.push(setState)
    return () => {
      const index = listeners.indexOf(setState)
      if (index > -1) {
        listeners.splice(index, 1)
      }
    }
  }, [state])

  return {
    ...state,
    toast,
    dismiss: (toastId?: string) => dispatch({ type: "DISMISS_TOAST", toastId }),
  }
}

export { useToast, toast }
