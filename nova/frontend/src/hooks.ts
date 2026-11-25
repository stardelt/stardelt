// Hooks more than one screen uses. A read that many pages share has its query key here, once.

import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { api } from './api'

/** The signed-in person (`GET /api/me`): one query key for every page and component. */
export function useMe() {
  return useQuery({ queryKey: ['me'], queryFn: api.me })
}

/** A value typed into a box, applied once the person stops typing for `ms` milliseconds. */
export function useDebounced<T>(value: T, ms: number): T {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), ms)
    return () => clearTimeout(timer)
  }, [value, ms])
  return debounced
}
