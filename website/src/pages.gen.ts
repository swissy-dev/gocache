// deno-fmt-ignore-file
// biome-ignore format: generated types do not need formatting
// prettier-ignore
import type { PathsForPages } from 'waku/router'

// prettier-ignore
type Page =
  | { path: '/bus/custom'; render: 'static' }
  | { path: '/bus/overview'; render: 'static' }
  | { path: '/concepts/expiry'; render: 'static' }
  | { path: '/concepts/keys-and-namespaces'; render: 'static' }
  | { path: '/concepts/lifecycle'; render: 'static' }
  | { path: '/concepts/serialization'; render: 'static' }
  | { path: '/concepts/tiers'; render: 'static' }
  | { path: '/drivers/choosing'; render: 'static' }
  | { path: '/drivers/custom'; render: 'static' }
  | { path: '/drivers/memory'; render: 'static' }
  | { path: '/drivers/null'; render: 'static' }
  | { path: '/drivers/redis'; render: 'static' }
  | { path: '/drivers/sql'; render: 'static' }
  | { path: '/features/events'; render: 'static' }
  | { path: '/features/get-or-set'; render: 'static' }
  | { path: '/features/grace-periods'; render: 'static' }
  | { path: '/features/locks'; render: 'static' }
  | { path: '/features/tags'; render: 'static' }
  | { path: '/features/timeouts'; render: 'static' }
  | { path: '/guides/caching-database-queries'; render: 'static' }
  | { path: '/guides/running-multiple-instances'; render: 'static' }
  | { path: '/guides/testing'; render: 'static' }
  | { path: '/'; render: 'static' }
  | { path: '/introduction/how-it-works'; render: 'static' }
  | { path: '/reference/errors'; render: 'static' }
  | { path: '/reference/operations'; render: 'static' }
  | { path: '/reference/options'; render: 'static' }

// prettier-ignore
declare module 'waku/router' {
  interface RouteConfig {
    paths: PathsForPages<Page>
  }
  interface CreatePagesConfig {
    pages: Page
  }
}
