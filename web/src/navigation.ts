import { Archive, CalendarClock, HardDrive, History, Home, KeyRound, RotateCcw, Server, Settings } from '@lucide/vue';

export const pages = [
  { key: 'dashboard', labelKey: 'nav.dashboard', icon: Home },
  { key: 'hosts', labelKey: 'nav.hosts', icon: Server },
  { key: 'credentials', labelKey: 'nav.credentials', icon: KeyRound },
  { key: 'jobs', labelKey: 'nav.jobs', icon: CalendarClock },
  { key: 'runs', labelKey: 'nav.runs', icon: History },
  { key: 'snapshots', labelKey: 'nav.snapshots', icon: Archive },
  { key: 'restore', labelKey: 'nav.restore', icon: RotateCcw },
  { key: 'storage', labelKey: 'nav.storage', icon: HardDrive },
  { key: 'settings', labelKey: 'nav.settings', icon: Settings }
] as const;

export type PageKey = (typeof pages)[number]['key'];
