import { UserManager, WebStorageStateStore } from 'oidc-client-ts'
import { runtimeConfig } from './runtime'

const development = runtimeConfig.devAuth ?? import.meta.env.VITE_DEV_AUTH !== 'false'

export const userManager = development ? null : new UserManager({
  authority: runtimeConfig.oidcAuthority ?? import.meta.env.VITE_OIDC_AUTHORITY ?? 'http://localhost:8180/realms/runmesh',
  client_id: runtimeConfig.oidcClientId ?? import.meta.env.VITE_OIDC_CLIENT_ID ?? 'runmesh-web',
  redirect_uri: window.location.origin,
  post_logout_redirect_uri: window.location.origin,
  response_type: 'code',
  scope: 'openid profile email',
  automaticSilentRenew: true,
  userStore: new WebStorageStateStore({ store: window.sessionStorage }),
})

export async function authenticate(): Promise<void> {
  if (!userManager) return
  if (new URLSearchParams(window.location.search).has('code')) {
    await userManager.signinRedirectCallback()
    window.history.replaceState({}, document.title, window.location.pathname)
  }
  const user = await userManager.getUser()
  if (!user || user.expired) await userManager.signinRedirect()
}

export async function bearerToken(): Promise<string | undefined> {
  const user = await userManager?.getUser()
  return user?.access_token
}
