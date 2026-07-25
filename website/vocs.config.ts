import { defineConfig } from 'vocs/config'

export default defineConfig({
  title: 'gocache',
  description:
    'Multi-tier caching for Go: an in-memory L1 in front of a distributed L2, kept in sync across instances by a message bus.',
  editLink: {
    pattern: 'https://github.com/swissy-dev/gocache/edit/main/website/src/pages/:path',
    text: 'Suggest changes to this page',
  },
  socials: [{ icon: 'github', link: 'https://github.com/swissy-dev/gocache' }],
  topNav: [
    { text: 'Docs', link: '/', match: '/' },
    { text: 'Reference', link: '/reference/options', match: '/reference' },
    { text: 'GitHub', link: 'https://github.com/swissy-dev/gocache' },
  ],
  sidebar: [
    {
      text: 'Introduction',
      items: [
        { text: 'Getting started', link: '/' },
        { text: 'How it works', link: '/introduction/how-it-works' },
      ],
    },
    {
      text: 'Core concepts',
      items: [
        { text: 'Tiers', link: '/concepts/tiers' },
        { text: 'Keys and namespaces', link: '/concepts/keys-and-namespaces' },
        { text: 'Expiry', link: '/concepts/expiry' },
        { text: 'Serialization', link: '/concepts/serialization' },
        { text: 'Lifecycle', link: '/concepts/lifecycle' },
      ],
    },
    {
      text: 'Guides',
      items: [
        { text: 'Caching database queries', link: '/guides/caching-database-queries' },
        { text: 'Running multiple instances', link: '/guides/running-multiple-instances' },
        { text: 'Testing', link: '/guides/testing' },
      ],
    },
    {
      text: 'Features',
      items: [
        { text: 'Cache-aside and stampedes', link: '/features/get-or-set' },
        { text: 'Tags', link: '/features/tags' },
        { text: 'Grace periods', link: '/features/grace-periods' },
        { text: 'Factory timeouts', link: '/features/timeouts' },
        { text: 'Atomic locks', link: '/features/locks' },
        { text: 'Events', link: '/features/events' },
      ],
    },
    {
      text: 'Drivers',
      items: [
        { text: 'Choosing a driver', link: '/drivers/choosing' },
        { text: 'Memory', link: '/drivers/memory' },
        { text: 'Redis', link: '/drivers/redis' },
        { text: 'SQL', link: '/drivers/sql' },
        { text: 'Null', link: '/drivers/null' },
        { text: 'Writing a driver', link: '/drivers/custom' },
      ],
    },
    {
      text: 'The bus',
      items: [
        { text: 'Overview', link: '/bus/overview' },
        { text: 'Writing a bus', link: '/bus/custom' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'Options', link: '/reference/options' },
        { text: 'Operations', link: '/reference/operations' },
        { text: 'Errors', link: '/reference/errors' },
      ],
    },
  ],
})
