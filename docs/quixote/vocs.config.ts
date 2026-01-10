import { defineConfig } from 'vocs'

export default defineConfig({
  title: 'quixote',
  description: 'Simple and flexible Rust-based EVM event indexer powered by DuckDB.',
  logoUrl: '/quixote.png',
  iconUrl: '/quixote.png',
  theme: {
    accentColor: {
      light: '#8158e8',
      dark: '#08b4d5',
    }
  },
  socials: [
    {
      icon: 'github',
      link: 'https://github.com/bilinearlabs/quixote',
    },
  ],
  topNav: [
    { text: 'Bilinear Labs', link: 'https://bilinearlabs.io' },
    { text: 'Docs', link: '/get-started' },
    { text: 'GitHub', link: 'https://github.com/bilinearlabs/quixote' },
  ],
  sidebar: [
    {
      text: 'Get Started',
      link: '/get-started',
    },
    {
      text: 'Examples',
      link: '/examples',
    },
    {
      text: 'API',
      link: '/api',
    },
    {
      text: 'Database Schema',
      link: '/database-schema',
    },
    {
      text: 'Configuration File',
      link: '/config-file',
    },
    {
      text: 'Frontend',
      link: '/frontend',
    },
    {
      text: 'Data Integrity',
      link: '/data-integrity',
    },
    {
      text: 'Development',
      link: '/development',
    },
  ],
})
