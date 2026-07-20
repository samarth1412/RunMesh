export type RuntimeConfig = {
  apiUrl?: string
  devAuth?: boolean
  oidcAuthority?: string
  oidcClientId?: string
}

declare global {
  interface Window {
    __RUNMESH_CONFIG__?: RuntimeConfig
  }
}

export const runtimeConfig = window.__RUNMESH_CONFIG__ ?? {}
