import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { Activity, Archive, CalendarClock, Database, HardDrive, History, KeyRound, Server, Shield, UploadCloud } from '@lucide/vue';
import {
  api,
  type Bootstrap,
  type Credential,
  type Health,
  type Host,
  type Job,
  type MaintenanceSettings,
  type MaintenanceRun,
  type RestoreResult,
  type RestoreTask,
  type Run,
  type RunLog,
  type Snapshot,
  type SnapshotTree,
  type SnapshotTreeEntry,
  type StorageHealth,
  type StorageMaintenance
} from '../api';
import { en, locale, setLocale, t, type MessageKey } from '../i18n';
import { pages, type PageKey } from '../navigation';

const agentContainerStateDir = '/var/lib/turbk-agent';
const defaultAgentBackupSchedule = '0 0 * * *';
type AgentSetupMethod = 'compose' | 'docker' | 'binary' | 'systemd';
type AgentRunMode = 'daemon' | 'once';
type AgentScheduleMode = 'hourly' | 'daily' | 'weekly' | 'custom';
const hostTabs = ['overview', 'access', 'jobs'] as const;
type HostTab = (typeof hostTabs)[number];

const agentScheduleMinuteOptions = Array.from({ length: 60 }, (_, value) => ({ value: String(value), label: String(value).padStart(2, '0') }));
const agentScheduleHourOptions = Array.from({ length: 24 }, (_, value) => ({ value: String(value), label: String(value).padStart(2, '0') }));

function shellQuote(value: string) {
  if (value === '') return "''";
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function yamlQuote(value: string) {
  return `'${value.replace(/'/g, `''`)}'`;
}

function systemdQuote(value: string) {
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/%/g, '%%')}"`;
}

function normalizeAgentPath(value: string) {
  const trimmed = value.trim();
  if (trimmed === '') return '';
  const absolute = trimmed.startsWith('/');
  const collapsed = trimmed.replace(/\/+/g, '/');
  const parts: string[] = [];
  for (const part of collapsed.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  if (absolute) return `/${parts.join('/')}` || '/';
  return parts.join('/');
}

function agentRootNested(parent: string, child: string) {
  if (parent === child) return false;
  if (parent === '/') return child !== '/';
  return child.startsWith(`${parent.replace(/\/+$/, '')}/`);
}

function cronFieldValid(field: string, minValue: number, maxValue: number) {
  return field.split(',').every((part) => cronPartValid(part.trim(), minValue, maxValue));
}

function cronPartValid(part: string, minValue: number, maxValue: number) {
  if (part === '') return false;
  const [rangePart, stepPart, ...rest] = part.split('/');
  if (rest.length > 0) return false;
  if (stepPart !== undefined) {
    if (!/^\d+$/.test(stepPart)) return false;
    const step = Number(stepPart);
    if (!Number.isInteger(step) || step <= 0) return false;
  }
  let start = minValue;
  let end = maxValue;
  if (rangePart === '*') {
    return true;
  }
  if (rangePart.includes('-')) {
    const pieces = rangePart.split('-');
    if (pieces.length !== 2) return false;
    if (!/^\d+$/.test(pieces[0]) || !/^\d+$/.test(pieces[1])) return false;
    start = Number(pieces[0]);
    end = Number(pieces[1]);
  } else {
    if (!/^\d+$/.test(rangePart)) return false;
    start = Number(rangePart);
    end = start;
  }
  return Number.isInteger(start) && Number.isInteger(end) && start >= minValue && end <= maxValue && start <= end;
}

function isCronExpression(value: string) {
  const trimmed = value.trim();
  if (trimmed === '') return false;
  if (['@hourly', '@daily', '@midnight', '@weekly'].includes(trimmed)) return true;
  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5) return false;
  const ranges = [
    [0, 59],
    [0, 23],
    [1, 31],
    [1, 12],
    [0, 7]
  ] as const;
  for (let i = 0; i < fields.length; i += 1) {
    if (!cronFieldValid(fields[i], ranges[i][0], ranges[i][1])) return false;
  }
  return true;
}

function parseCronNumber(value: string, minValue: number, maxValue: number) {
  if (!/^\d+$/.test(value)) return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minValue || parsed > maxValue) return null;
  return String(parsed);
}

function parseAgentBackupSchedule(value: string) {
  const trimmed = value.trim();
  switch (trimmed) {
    case '@hourly':
      return { mode: 'hourly' as const, minute: '0', hour: '0', weekday: '0' };
    case '@daily':
    case '@midnight':
      return { mode: 'daily' as const, minute: '0', hour: '0', weekday: '0' };
    case '@weekly':
      return { mode: 'weekly' as const, minute: '0', hour: '0', weekday: '0' };
  }
  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5) return { mode: 'custom' as const };
  const minute = parseCronNumber(fields[0], 0, 59);
  const hour = parseCronNumber(fields[1], 0, 23);
  if (minute !== null && fields[1] === '*' && fields[2] === '*' && fields[3] === '*' && fields[4] === '*') {
    return { mode: 'hourly' as const, minute, hour: '0', weekday: '0' };
  }
  if (minute !== null && hour !== null && fields[2] === '*' && fields[3] === '*' && fields[4] === '*') {
    return { mode: 'daily' as const, minute, hour, weekday: '0' };
  }
  const weekday = parseCronNumber(fields[4], 0, 7);
  if (minute !== null && hour !== null && fields[2] === '*' && fields[3] === '*' && weekday !== null) {
    return { mode: 'weekly' as const, minute, hour, weekday: weekday === '7' ? '0' : weekday };
  }
  return { mode: 'custom' as const };
}

function agentBackupScheduleFromParts(mode: AgentScheduleMode, minute: string, hour: string, weekday: string) {
  switch (mode) {
    case 'hourly':
      return `${minute} * * * *`;
    case 'daily':
      return `${minute} ${hour} * * *`;
    case 'weekly':
      return `${minute} ${hour} * * ${weekday}`;
    case 'custom':
      return null;
  }
}

function isHostTab(value: string | null): value is HostTab {
  return hostTabs.includes(value as HostTab);
}

export function useBackupApp() {
  const pageKeys = new Set<PageKey>(pages.map((page) => page.key));

  function pageFromPath(pathname: string): PageKey {
    const segment = pathname.split('/').filter(Boolean)[0] ?? '';
    if (pageKeys.has(segment as PageKey)) return segment as PageKey;
    return 'dashboard';
  }

  function pagePath(page: PageKey) {
    return `/${page}`;
  }

  function hostPagePath(hostID = selectedHostId.value, tab = activeHostTab.value) {
    const params = new URLSearchParams();
    if (hostID !== null) params.set('host', String(hostID));
    params.set('tab', tab);
    const query = params.toString();
    return `${pagePath('hosts')}${query ? `?${query}` : ''}`;
  }

  function initialPage(): PageKey {
    if (typeof window === 'undefined') return 'dashboard';
    return pageFromPath(window.location.pathname);
  }

  const activePage = ref<PageKey>(initialPage());
  const loading = ref(false);
  const error = ref('');
  const health = ref<Health | null>(null);
  const bootstrap = ref<Bootstrap | null>(null);
  const storageHealth = ref<StorageHealth | null>(null);
  const maintenanceReport = ref<StorageMaintenance | null>(null);
  const hosts = ref<Host[]>([]);
  const credentials = ref<Credential[]>([]);
  const jobs = ref<Job[]>([]);
  const runs = ref<Run[]>([]);
  const snapshots = ref<Snapshot[]>([]);
  const restoreTasks = ref<RestoreTask[]>([]);
  const maintenanceRuns = ref<MaintenanceRun[]>([]);
  const selectedSnapshot = ref<Snapshot | null>(null);
  const selectedSnapshotIds = ref<number[]>([]);
  const snapshotDeleteTarget = ref<Snapshot | null>(null);
  const snapshotDeleteMany = ref(false);
  const deletingSnapshots = ref(false);
  const snapshotTreePath = ref('.');
  const snapshotTreeEntries = ref<SnapshotTreeEntry[]>([]);
  const snapshotTreeManifest = ref<SnapshotTree['manifest'] | null>(null);
  const hostName = ref('');
  const hostSourceType = ref('local');
  const hostAddress = ref('');
  const hostCredentialId = ref<number | ''>('');
  const showHostCreate = ref(false);
  const hostActionMessage = ref('');
  const selectedHostId = ref<number | null>(null);
  const activeHostTab = ref<HostTab>('overview');
  const hostSearch = ref('');
  const hostSourceFilter = ref('all');
  const agentSetupHostName = ref('');
  const agentSetupSourceDirs = ref(['/srv/data']);
  const agentSetupBackupSchedule = ref(defaultAgentBackupSchedule);
  const agentSetupScheduleMode = ref<AgentScheduleMode>('daily');
  const agentSetupScheduleMinute = ref('0');
  const agentSetupScheduleHour = ref('0');
  const agentSetupScheduleWeekday = ref('0');
  const agentSetupSaveMessage = ref('');
  const agentSetupSourceDir = computed({
    get: () => agentSetupSourceDirs.value[0] ?? '',
    set: (value: string) => {
      agentSetupSourceDirs.value = [value];
    }
  });
  const agentSetupRunMode = ref<AgentRunMode>('daemon');
  const agentSetupMethod = ref<AgentSetupMethod>('compose');
  const copyActionMessage = ref('');
  const credentialName = ref('');
  const credentialType = ref('sftp');
  const credentialUsername = ref('');
  const credentialPassword = ref('');
  const credentialPrivateKey = ref('');
  const credentialBearerToken = ref('');
  const credentialExplicitTLS = ref(true);
  const credentialSkipTLSVerify = ref(false);
  const showCredentialCreate = ref(false);
  const selectedCredentialId = ref<number | null>(null);
  const agentCredentialClientId = ref('');
  const agentCredentialSecret = ref('');
  const jobName = ref('');
  const jobRoot = ref('');
  const jobHostId = ref<number | ''>('');
  const jobSchedule = ref('');
  const jobTimezone = ref('Asia/Shanghai');
  const jobMaxRuntimeSeconds = ref(0);
  const jobRetryAttempts = ref(0);
  const showJobCreate = ref(false);
  const jobActionMessage = ref('');
  const restoreSnapshotId = ref<number | ''>('');
  const restoreEntryPath = ref('.');
  const restoreTargetPath = ref('');
  const restoreResult = ref<RestoreResult | null>(null);
  const runningJobId = ref<number | null>(null);
  const loadingTree = ref(false);
  const restoring = ref(false);
  const maintenanceRunning = ref(false);
  const actionMessage = ref('');
  const checkingSession = ref(true);
  const authenticated = ref(false);
  const currentUser = ref('');
  const loginUsername = ref('admin');
  const loginPassword = ref('');
  const loginLoading = ref(false);
  const loginError = ref('');
  const editJobId = ref<number | null>(null);
  const editJobName = ref('');
  const editJobRoot = ref('');
  const editJobSchedule = ref('');
  const editJobTimezone = ref('Asia/Shanghai');
  const editJobMaxRuntimeSeconds = ref(0);
  const editJobRetryAttempts = ref(0);
  const editJobEnabled = ref(true);
  const savingJobId = ref<number | null>(null);
  const selectedRunId = ref<number | null>(null);
  const runLogs = ref<RunLog[]>([]);
  const loadingRunLogs = ref(false);
  const settingsAdminUsername = ref('admin');
  const settingsSessionTTLHours = ref(24);
  const settingsCurrentPassword = ref('');
  const settingsNewPassword = ref('');
  const settingsKeepLast = ref(30);
  const settingsKeepDaily = ref(30);
  const settingsKeepWeekly = ref(12);
  const settingsMaintenanceEnabled = ref(true);
  const settingsMaintenanceTimezone = ref('Asia/Shanghai');
  const settingsCleanupSchedule = ref('0 3 * * *');
  const settingsCompactEnabled = ref(true);
  const settingsCompactSchedule = ref('30 3 * * 0');
  const settingsErrorGracePeriod = ref('24h');
  const settingsStaleRunAfter = ref('6h');
  const settingsKeepDeletedMetadataDays = ref(30);
  const settingsCompactMinReclaimRatio = ref(0.15);
  const settingsCompactMinReclaimBytes = ref('1GiB');
  const savingSettings = ref(false);
  const savingAgentSetup = ref(false);
  let suppressAgentSetupSave = false;
  let syncingAgentSetupScheduleEditor = false;
  let agentSetupSaveTimer: ReturnType<typeof setTimeout> | null = null;
  let lastSavedAgentSetupKey = '';

  const activeMeta = computed(() => pages.find((page) => page.key === activePage.value) ?? pages[0]);
  const activeTitle = computed(() => t(activeMeta.value.labelKey));
  const counts = computed(() => bootstrap.value?.counts ?? { hosts: 0, credentials: 0, jobs: 0, runs: 0, snapshots: 0 });

  function arrayOrEmpty<T>(value: T[] | null | undefined): T[] {
    return Array.isArray(value) ? value : [];
  }

  function normalizeBootstrap(value: Bootstrap): Bootstrap {
    return {
      ...value,
      counts: value.counts ?? { hosts: 0, credentials: 0, jobs: 0, runs: 0, snapshots: 0 },
      paths: {
        state_dir: value.paths?.state_dir ?? '',
        repo_dir: value.paths?.repo_dir ?? '',
        restore_roots: arrayOrEmpty(value.paths?.restore_roots)
      },
      repository: value.repository ?? {},
      scheduler: {
        timezone: value.scheduler?.timezone ?? 'Asia/Shanghai',
        max_concurrent_runs: value.scheduler?.max_concurrent_runs ?? 0
      },
      auth: {
        username: value.auth?.username ?? 'admin',
        session_ttl_hours: value.auth?.session_ttl_hours ?? 24
      },
      retention: {
        keep_last: value.retention?.keep_last ?? 30,
        keep_daily: value.retention?.keep_daily ?? 30,
        keep_weekly: value.retention?.keep_weekly ?? 12
      },
      maintenance: {
        enabled: value.maintenance?.enabled ?? true,
        timezone: value.maintenance?.timezone ?? 'Asia/Shanghai',
        cleanup_schedule: value.maintenance?.cleanup_schedule ?? '0 3 * * *',
        compact_enabled: value.maintenance?.compact_enabled ?? true,
        compact_schedule: value.maintenance?.compact_schedule ?? '30 3 * * 0',
        error_grace_period: value.maintenance?.error_grace_period ?? '24h',
        stale_run_after: value.maintenance?.stale_run_after ?? '6h',
        keep_deleted_metadata_days: value.maintenance?.keep_deleted_metadata_days ?? 30,
        compact_min_reclaim_ratio: value.maintenance?.compact_min_reclaim_ratio ?? 0.15,
        compact_min_reclaim_bytes: value.maintenance?.compact_min_reclaim_bytes ?? '1GiB'
      }
    };
  }

  function navigatePage(page: PageKey, replace = false) {
    activePage.value = page;
    if (typeof window === 'undefined') return;
    const nextPath = page === 'hosts' ? hostPagePath() : pagePath(page);
    if (`${window.location.pathname}${window.location.search}` === nextPath) return;
    if (replace) {
      window.history.replaceState({ page }, '', nextPath);
    } else {
      window.history.pushState({ page }, '', nextPath);
    }
  }

  function syncPageFromLocation() {
    const page = pageFromPath(window.location.pathname);
    activePage.value = page;
    const segment = window.location.pathname.split('/').filter(Boolean)[0] ?? '';
    if (segment !== '' && !pageKeys.has(segment as PageKey)) {
      window.history.replaceState({ page }, '', pagePath(page));
    }
    if (page === 'hosts') syncHostStateFromLocation();
  }

  function syncHostStateFromLocation() {
    if (typeof window === 'undefined') return;
    const params = new URLSearchParams(window.location.search);
    const queryHostID = Number(params.get('host'));
    if (Number.isInteger(queryHostID) && queryHostID > 0) {
      selectedHostId.value = queryHostID;
    }
    const queryTab = params.get('tab');
    activeHostTab.value = isHostTab(queryTab) ? queryTab : 'overview';
  }

  function updateHostLocation(replace = false) {
    if (typeof window === 'undefined' || activePage.value !== 'hosts') return;
    const nextPath = hostPagePath();
    if (`${window.location.pathname}${window.location.search}` === nextPath) return;
    const state = { page: 'hosts', host: selectedHostId.value, tab: activeHostTab.value };
    if (replace) {
      window.history.replaceState(state, '', nextPath);
    } else {
      window.history.pushState(state, '', nextPath);
    }
  }

  const statRows = computed(() => [
    { label: t('nav.hosts'), value: counts.value.hosts, detail: t('dashboard.hostsDetail'), icon: Server },
    { label: t('nav.jobs'), value: counts.value.jobs, detail: t('dashboard.jobsDetail'), icon: CalendarClock },
    { label: t('nav.runs'), value: counts.value.runs, detail: t('dashboard.runsDetail'), icon: History },
    { label: t('nav.snapshots'), value: counts.value.snapshots, detail: t('dashboard.snapshotsDetail'), icon: Archive }
  ]);

  const hostSummaryRows = computed(() => [
    { label: t('hosts.total'), value: hosts.value.length, detail: t('hosts.selectHost'), icon: Server },
    {
      label: t('hosts.agentOnline'),
      value: hosts.value.filter((host) => host.source_type === 'agent' && host.status === 'online').length,
      detail: `${hosts.value.filter((host) => host.source_type === 'agent').length} ${sourceTypeLabel('agent')}`,
      icon: Activity
    },
    {
      label: t('hosts.pullSources'),
      value: hosts.value.filter((host) => !['agent', 'local'].includes(host.source_type)).length,
      detail: 'SFTP / FTP / FTPS / WebDAV',
      icon: UploadCloud
    },
    {
      label: t('hosts.managedJobs'),
      value: jobs.value.length,
      detail: t('dashboard.jobsDetail'),
      icon: CalendarClock
    }
  ]);

  const hostSourceOptions = computed(() => [
    { value: 'agent', label: sourceTypeLabel('agent'), title: t('hosts.agentPush'), description: t('hosts.agentPushDesc'), icon: Activity },
    { value: 'sftp', label: 'SFTP', title: t('hosts.sftpPull'), description: t('hosts.sftpPullDesc'), icon: Server },
    { value: 'ftp', label: 'FTP', title: t('hosts.ftpPull'), description: t('hosts.ftpPullDesc'), icon: UploadCloud },
    { value: 'ftps', label: 'FTPS', title: t('hosts.ftpsPull'), description: t('hosts.ftpsPullDesc'), icon: Shield },
    { value: 'webdav', label: 'WebDAV', title: t('hosts.webdavPull'), description: t('hosts.webdavPullDesc'), icon: Database },
    { value: 'local', label: sourceTypeLabel('local'), title: t('hosts.localMount'), description: t('hosts.localMountDesc'), icon: HardDrive }
  ]);

  const hostSourceFilterOptions = computed(() => [
    { value: 'all', label: t('common.all') },
    ...hostSourceOptions.value.map((option) => ({ value: option.value, label: option.label }))
  ]);

  const filteredHosts = computed(() => {
    const sourceFilter = hostSourceFilter.value;
    const query = hostSearch.value.trim().toLowerCase();
    return hosts.value.filter((host) => {
      if (sourceFilter !== 'all' && host.source_type !== sourceFilter) return false;
      if (query === '') return true;
      const fields = [
        String(host.id),
        host.name,
        host.source_type,
        sourceTypeLabel(host.source_type),
        statusText(host.status),
        hostAddressText(host)
      ];
      return fields.some((field) => field.toLowerCase().includes(query));
    });
  });

  const hostTabOptions = computed(() => [
    { value: 'overview' as const, label: t('hosts.tabOverview') },
    { value: 'access' as const, label: t('hosts.tabAccess') },
    { value: 'jobs' as const, label: t('hosts.tabJobs') }
  ]);

  const credentialSourceOptions = computed(() => hostSourceOptions.value.filter((option) => !['agent', 'local'].includes(option.value)));

  const credentialSummaryRows = computed(() => {
    const linkedJobCount = credentials.value.reduce((total, credential) => total + jobsForCredential(credential).length, 0);
    return [
      { label: t('credentials.inventory'), value: credentials.value.length, detail: t('credentials.inventoryDesc'), icon: KeyRound },
      {
        label: t('credentials.pullCount'),
        value: credentials.value.length,
        detail: 'SFTP / FTP / FTPS / WebDAV',
        icon: UploadCloud
      },
      { label: t('credentials.linkedJobs'), value: linkedJobCount, detail: t('dashboard.jobsDetail'), icon: CalendarClock }
    ];
  });

  const selectedHost = computed(() => {
    if (hosts.value.length === 0) return null;
    return hosts.value.find((host) => host.id === selectedHostId.value) ?? hosts.value[0];
  });

  const selectedHostCredentials = computed(() => {
    const host = selectedHost.value;
    if (!host) return [];
    if (host.source_type === 'local') return [];
    const credential = credentialForHost(host);
    return credential ? [credential] : [];
  });

  const selectedHostJobs = computed(() => {
    const host = selectedHost.value;
    if (!host) return [];
    return jobs.value.filter((job) => nullableNumber(job.host_id) === host.id);
  });

  const selectedAgentCredential = computed(() => {
    const host = selectedHost.value;
    if (!host || host.source_type !== 'agent') return null;
    return host.agent ?? null;
  });

  const selectedCredential = computed(() => {
    if (credentials.value.length === 0) return null;
    return credentials.value.find((credential) => credential.id === selectedCredentialId.value) ?? credentials.value[0];
  });

  const selectedCredentialJobs = computed(() => {
    const credential = selectedCredential.value;
    if (!credential) return [];
    return jobsForCredential(credential);
  });

  const selectedCredentialHosts = computed(() => {
    const credential = selectedCredential.value;
    if (!credential) return [];
    return hosts.value.filter((host) => nullableNumber(host.credential_id) === credential.id);
  });

  const activeAgentCredential = computed(() => {
    return selectedAgentCredential.value;
  });

  const currentServerURL = computed(() => {
    if (typeof window === 'undefined') return 'http://localhost:8080';
    return window.location.origin;
  });

  const agentSetupClientId = computed(() => activeAgentCredential.value?.client_id || agentCredentialClientId.value || 'agt_replace_me');
  const agentSetupClientSecret = computed(() => activeAgentCredential.value?.client_secret || agentCredentialSecret.value || 'ags_replace_me');
  const jobHostOptions = computed(() => hosts.value.filter((host) => host.source_type !== 'agent'));

  const agentSetupRoots = computed(() => agentSetupSourceDirs.value.map((root) => normalizeAgentPath(root)).filter((root) => root !== ''));

  const agentSetupRootErrors = computed(() => {
    const roots = agentSetupSourceDirs.value.map((root) => normalizeAgentPath(root)).filter((root) => root !== '');
    const errors: string[] = [];
    if (roots.length === 0) {
      errors.push(t('hosts.agentRootRequiredError'));
      return errors;
    }
    const seen = new Set<string>();
    for (const root of roots) {
      if (!root.startsWith('/')) {
        errors.push(`${root}: ${t('hosts.agentRootAbsoluteError')}`);
        continue;
      }
      if (seen.has(root)) {
        errors.push(`${root}: ${t('hosts.agentRootDuplicateError')}`);
      }
      seen.add(root);
    }
    for (let i = 0; i < roots.length; i += 1) {
      for (let j = 0; j < roots.length; j += 1) {
        if (i !== j && roots[i].startsWith('/') && roots[j].startsWith('/') && agentRootNested(roots[i], roots[j])) {
          errors.push(`${roots[j]}: ${t('hosts.agentRootNestedError')} ${roots[i]}`);
        }
      }
    }
    return errors;
  });

  const agentSetupBackupScheduleValue = computed(() => agentSetupBackupSchedule.value.trim() || defaultAgentBackupSchedule);
  const agentSetupScheduleErrors = computed(() => {
    if (agentSetupRunMode.value !== 'daemon') return [];
    return isCronExpression(agentSetupBackupScheduleValue.value) ? [] : [t('hosts.agentBackupScheduleError')];
  });
  const agentSetupReady = computed(() => agentSetupRootErrors.value.length === 0 && agentSetupScheduleErrors.value.length === 0);
  const agentRootEnvValue = computed(() => agentSetupRoots.value.join(','));
  const agentRootFlags = computed(() => agentSetupRoots.value.map((root) => `-root ${shellQuote(root)}`).join(' '));
  const agentSystemdRootFlags = computed(() => agentSetupRoots.value.map((root) => `-root ${systemdQuote(root)}`).join(' '));
  const agentSetupScheduleModeOptions = computed(() => [
    { value: 'hourly' as const, label: t('hosts.agentScheduleHourly') },
    { value: 'daily' as const, label: t('hosts.agentScheduleDaily') },
    { value: 'weekly' as const, label: t('hosts.agentScheduleWeekly') },
    { value: 'custom' as const, label: t('hosts.agentScheduleCustom') }
  ]);
  const agentSetupScheduleWeekdayOptions = computed(() => [
    { value: '0', label: t('weekday.sunday') },
    { value: '1', label: t('weekday.monday') },
    { value: '2', label: t('weekday.tuesday') },
    { value: '3', label: t('weekday.wednesday') },
    { value: '4', label: t('weekday.thursday') },
    { value: '5', label: t('weekday.friday') },
    { value: '6', label: t('weekday.saturday') }
  ]);
  const agentRunModeOptions = computed(() => [
    { value: 'daemon' as const, label: t('hosts.agentRunDaemon'), description: t('hosts.agentRunDaemonDesc') },
    { value: 'once' as const, label: t('hosts.agentRunOnce'), description: t('hosts.agentRunOnceDesc') }
  ]);

  function addAgentSetupSourceDir() {
    agentSetupSourceDirs.value.push('');
  }

  function removeAgentSetupSourceDir(index: number) {
    if (agentSetupSourceDirs.value.length <= 1) {
      agentSetupSourceDirs.value = [''];
      return;
    }
    agentSetupSourceDirs.value.splice(index, 1);
  }

  const agentComposeEnv = computed(() =>
    [
      `TURBK_SERVER_URL=${currentServerURL.value}`,
      `TURBK_AGENT_ID=${agentSetupClientId.value}`,
      `TURBK_AGENT_SECRET=${agentSetupClientSecret.value}`,
      ...(agentSetupRunMode.value === 'daemon' ? [`TURBK_AGENT_DAEMON=true`] : []),
      ...(agentSetupRunMode.value === 'daemon' ? [`TURBK_AGENT_BACKUP_SCHEDULE=${agentSetupBackupScheduleValue.value}`] : []),
      `TURBK_AGENT_STATE_DIR=${agentContainerStateDir}`,
      `TURBK_AGENT_ROOTS=${agentRootEnvValue.value}`
    ].join('\n')
  );
  const agentSetupStateMount = computed(() => `./agent-state:${agentContainerStateDir}`);
  const agentSetupSourceMounts = computed(() => agentSetupRoots.value.map((root) => `${root}:${root}:ro`));
  const agentSetupSourceMount = computed(() => agentSetupSourceMounts.value.join('\n'));

  const agentComposeSetup = computed(() => {
    const environment = [
      `      TURBK_SERVER_URL: ${yamlQuote(currentServerURL.value)}`,
      `      TURBK_AGENT_ID: ${yamlQuote(agentSetupClientId.value)}`,
      `      TURBK_AGENT_SECRET: ${yamlQuote(agentSetupClientSecret.value)}`,
      ...(agentSetupRunMode.value === 'daemon' ? ['      TURBK_AGENT_DAEMON: "true"'] : []),
      ...(agentSetupRunMode.value === 'daemon' ? [`      TURBK_AGENT_BACKUP_SCHEDULE: ${yamlQuote(agentSetupBackupScheduleValue.value)}`] : []),
      `      TURBK_AGENT_STATE_DIR: ${yamlQuote(agentContainerStateDir)}`,
      `      TURBK_AGENT_ROOTS: ${yamlQuote(agentRootEnvValue.value)}`
    ];
    const volumes = [
      `      - ${yamlQuote(`./agent-state:${agentContainerStateDir}`)}`,
      ...agentSetupRoots.value.map((root) => `      - ${yamlQuote(`${root}:${root}:ro`)}`)
    ];
    return [
      'services:',
      '  turbk-agent:',
      '    image: ghcr.io/tursom/turbk-agent:latest',
      '    user: "0:0"',
      '    environment:',
      ...environment,
      '    volumes:',
      ...volumes,
      ...(agentSetupRunMode.value === 'daemon' ? ['    restart: unless-stopped'] : [])
    ].join('\n');
  });

  const agentDockerCommand = computed(() => {
    const daemon = agentSetupRunMode.value === 'daemon';
    return [
      daemon ? 'docker run -d --name turbk-agent --restart unless-stopped \\' : 'docker run --rm --name turbk-agent-once \\',
      `  -e TURBK_SERVER_URL=${JSON.stringify(currentServerURL.value)} \\`,
      `  -e TURBK_AGENT_ID=${JSON.stringify(agentSetupClientId.value)} \\`,
      `  -e TURBK_AGENT_SECRET=${JSON.stringify(agentSetupClientSecret.value)} \\`,
      ...(daemon ? ['  -e TURBK_AGENT_DAEMON=true \\'] : []),
      ...(daemon ? [`  -e TURBK_AGENT_BACKUP_SCHEDULE=${JSON.stringify(agentSetupBackupScheduleValue.value)} \\`] : []),
      `  -e TURBK_AGENT_STATE_DIR=${JSON.stringify(agentContainerStateDir)} \\`,
      `  -e TURBK_AGENT_ROOTS=${JSON.stringify(agentRootEnvValue.value)} \\`,
      `  -v ${JSON.stringify(`turbk-agent-state:${agentContainerStateDir}`)} \\`,
      ...agentSetupRoots.value.map((root) => `  -v ${JSON.stringify(`${root}:${root}:ro`)} \\`),
      daemon ? '  ghcr.io/tursom/turbk-agent:latest' : '  ghcr.io/tursom/turbk-agent:latest -once'
    ].join('\n');
  });

  const agentBinaryCommand = computed(() =>
    [
      '# Put the turbk-agent binary on this host, then run:',
      `export TURBK_SERVER_URL=${shellQuote(currentServerURL.value)}`,
      `export TURBK_AGENT_ID=${shellQuote(agentSetupClientId.value)}`,
      `export TURBK_AGENT_SECRET=${shellQuote(agentSetupClientSecret.value)}`,
      `export TURBK_AGENT_STATE_DIR=${shellQuote('/var/lib/turbk-agent')}`,
      ...(agentSetupRunMode.value === 'daemon' ? [`export TURBK_AGENT_BACKUP_SCHEDULE=${shellQuote(agentSetupBackupScheduleValue.value)}`] : []),
      `./turbk-agent ${agentRootFlags.value} ${agentSetupRunMode.value === 'daemon' ? '-daemon' : '-once'}`
    ].join('\n')
  );

  const agentSystemdTimer = computed(() => {
    const daemon = agentSetupRunMode.value === 'daemon';
    const unitName = daemon ? 'turbk-agent.service' : 'turbk-agent-once.service';
    return [
      'sudo install -m 0755 ./turbk-agent /usr/local/bin/turbk-agent',
      `sudo tee /etc/systemd/system/${unitName} >/dev/null <<'UNIT'`,
      '[Unit]',
      daemon ? 'Description=Turbk agent backup daemon' : 'Description=Turbk agent one-shot backup',
      'After=network-online.target',
      'Wants=network-online.target',
      '',
      '[Service]',
      daemon ? 'Type=simple' : 'Type=oneshot',
      `Environment=${systemdQuote(`TURBK_SERVER_URL=${currentServerURL.value}`)}`,
      `Environment=${systemdQuote(`TURBK_AGENT_ID=${agentSetupClientId.value}`)}`,
      `Environment=${systemdQuote(`TURBK_AGENT_SECRET=${agentSetupClientSecret.value}`)}`,
      `Environment=${systemdQuote(`TURBK_AGENT_STATE_DIR=/var/lib/turbk-agent`)}`,
      ...(daemon ? [`Environment=${systemdQuote(`TURBK_AGENT_BACKUP_SCHEDULE=${agentSetupBackupScheduleValue.value}`)}`] : []),
      `ExecStart=/usr/local/bin/turbk-agent ${agentSystemdRootFlags.value} ${daemon ? '-daemon' : '-once'}`,
      ...(daemon ? ['Restart=always', 'RestartSec=30'] : []),
      'UNIT',
      '',
      'sudo systemctl daemon-reload',
      daemon ? `sudo systemctl enable --now ${unitName}` : `sudo systemctl start ${unitName}`
    ].join('\n');
  });

  const agentRunModeLabel = computed(() => (agentSetupRunMode.value === 'daemon' ? t('hosts.agentRunDaemon') : t('hosts.agentRunOnce')));

  const agentSetupMethods = computed(() => [
    {
      value: 'compose' as const,
      title: t('hosts.agentMethodCompose'),
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodComposeDaemonDesc' : 'hosts.agentMethodComposeOnceDesc')
    },
    {
      value: 'docker' as const,
      title: t('hosts.agentMethodDocker'),
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodDockerDaemonDesc' : 'hosts.agentMethodDockerOnceDesc')
    },
    {
      value: 'binary' as const,
      title: t('hosts.agentMethodBinary'),
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodBinaryDaemonDesc' : 'hosts.agentMethodBinaryOnceDesc')
    },
    {
      value: 'systemd' as const,
      title: t('hosts.agentMethodSystemd'),
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodSystemdDaemonDesc' : 'hosts.agentMethodSystemdOnceDesc')
    }
  ]);

  const agentSetupSnippets = computed<Record<AgentSetupMethod, { title: string; description: string; code: string }>>(() => ({
    compose: {
      title: `${t('hosts.agentMethodCompose')} · ${agentRunModeLabel.value}`,
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodComposeDaemonDesc' : 'hosts.agentMethodComposeOnceDesc'),
      code: agentComposeSetup.value
    },
    docker: {
      title: `${t('hosts.agentMethodDocker')} · ${agentRunModeLabel.value}`,
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodDockerDaemonDesc' : 'hosts.agentMethodDockerOnceDesc'),
      code: agentDockerCommand.value
    },
    binary: {
      title: `${t('hosts.agentMethodBinary')} · ${agentRunModeLabel.value}`,
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodBinaryDaemonDesc' : 'hosts.agentMethodBinaryOnceDesc'),
      code: agentBinaryCommand.value
    },
    systemd: {
      title: `${t('hosts.agentMethodSystemd')} · ${agentRunModeLabel.value}`,
      description: t(agentSetupRunMode.value === 'daemon' ? 'hosts.agentMethodSystemdDaemonDesc' : 'hosts.agentMethodSystemdOnceDesc'),
      code: agentSystemdTimer.value
    }
  }));

  const selectedAgentSetupSnippet = computed(() => agentSetupSnippets.value[agentSetupMethod.value]);

  function syncAgentSetupScheduleEditor(value: string) {
    const parsed = parseAgentBackupSchedule(value);
    syncingAgentSetupScheduleEditor = true;
    try {
      agentSetupScheduleMode.value = parsed.mode;
      if (parsed.mode !== 'custom') {
        agentSetupScheduleMinute.value = parsed.minute;
        agentSetupScheduleHour.value = parsed.hour;
        agentSetupScheduleWeekday.value = parsed.weekday;
      }
    } finally {
      syncingAgentSetupScheduleEditor = false;
    }
  }

  function applyAgentSetupScheduleEditor() {
    if (syncingAgentSetupScheduleEditor) return;
    const next = agentBackupScheduleFromParts(
      agentSetupScheduleMode.value,
      agentSetupScheduleMinute.value,
      agentSetupScheduleHour.value,
      agentSetupScheduleWeekday.value
    );
    if (next !== null && next !== agentSetupBackupSchedule.value) {
      agentSetupBackupSchedule.value = next;
    }
  }

  function hydrateAgentSetupFromHost(host: Host | null) {
    suppressAgentSetupSave = true;
    if (host?.source_type === 'agent') {
      const roots = Array.isArray(host.agent_setup?.roots) && host.agent_setup.roots.length > 0 ? host.agent_setup.roots : ['/srv/data'];
      agentSetupSourceDirs.value = roots.slice();
      agentSetupBackupSchedule.value = host.agent_setup?.backup_schedule || defaultAgentBackupSchedule;
      lastSavedAgentSetupKey = agentSetupSaveKey(host.id);
    } else {
      agentSetupSourceDirs.value = ['/srv/data'];
      agentSetupBackupSchedule.value = defaultAgentBackupSchedule;
      lastSavedAgentSetupKey = '';
    }
    agentSetupSaveMessage.value = '';
    suppressAgentSetupSave = false;
  }

  function agentSetupPayload() {
    return {
      roots: agentSetupRoots.value,
      backup_schedule: agentSetupBackupScheduleValue.value
    };
  }

  function agentSetupSaveKey(hostID = selectedHost.value?.id ?? 0) {
    return `${hostID}:${JSON.stringify(agentSetupPayload())}`;
  }

  function scheduleAgentSetupSave() {
    if (suppressAgentSetupSave) return;
    const host = selectedHost.value;
    if (!host || host.source_type !== 'agent') return;
    if (agentSetupRootErrors.value.length > 0 || agentSetupScheduleErrors.value.length > 0) return;
    if (agentSetupSaveTimer) clearTimeout(agentSetupSaveTimer);
    agentSetupSaveTimer = setTimeout(() => {
      agentSetupSaveTimer = null;
      void saveAgentSetup();
    }, 500);
  }

  async function saveAgentSetup() {
    const host = selectedHost.value;
    if (!host || host.source_type !== 'agent') return;
    if (agentSetupRootErrors.value.length > 0 || agentSetupScheduleErrors.value.length > 0) return;
    const saveKey = agentSetupSaveKey(host.id);
    if (saveKey === lastSavedAgentSetupKey) return;
    savingAgentSetup.value = true;
    agentSetupSaveMessage.value = t('common.saving');
    try {
      const response = await api.updateHost(host.id, { agent_setup: agentSetupPayload() });
      const index = hosts.value.findIndex((item) => item.id === response.host.id);
      if (index >= 0) hosts.value[index] = response.host;
      lastSavedAgentSetupKey = saveKey;
      agentSetupSaveMessage.value = t('message.agentSetupSaved');
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      agentSetupSaveMessage.value = error.value;
    } finally {
      savingAgentSetup.value = false;
    }
  }

  watch(agentSetupBackupSchedule, (value) => syncAgentSetupScheduleEditor(value), { immediate: true, flush: 'sync' });

  watch([agentSetupScheduleMode, agentSetupScheduleMinute, agentSetupScheduleHour, agentSetupScheduleWeekday], applyAgentSetupScheduleEditor, { flush: 'sync' });

  watch(selectedHost, (host) => hydrateAgentSetupFromHost(host), { immediate: true, flush: 'sync' });

  watch([agentSetupSourceDirs, agentSetupBackupSchedule], scheduleAgentSetupSave, { deep: true, flush: 'sync' });

  watch(hosts, (nextHosts) => {
    if (nextHosts.length === 0) {
      selectedHostId.value = null;
      jobHostId.value = '';
      updateHostLocation(true);
      return;
    }
    if (selectedHostId.value === null || !nextHosts.some((host) => host.id === selectedHostId.value)) {
      selectedHostId.value = nextHosts[0].id;
      updateHostLocation(true);
    }
    const validJobHosts = nextHosts.filter((host) => host.source_type !== 'agent');
    if (validJobHosts.length === 0) {
      jobHostId.value = '';
    } else if (jobHostId.value === '' || !validJobHosts.some((host) => host.id === jobHostId.value)) {
      jobHostId.value = validJobHosts[0].id;
    }
  });

  watch(credentials, (nextCredentials) => {
    if (nextCredentials.length === 0) {
      selectedCredentialId.value = null;
      hostCredentialId.value = '';
      return;
    }
    if (selectedCredentialId.value === null || !nextCredentials.some((credential) => credential.id === selectedCredentialId.value)) {
      selectedCredentialId.value = nextCredentials[0].id;
    }
    if (!['agent', 'local'].includes(hostSourceType.value)) {
      const compatible = nextCredentials.filter((credential) => credential.type === hostSourceType.value);
      if (compatible.length === 0) {
        hostCredentialId.value = '';
      } else if (hostCredentialId.value === '' || !compatible.some((credential) => credential.id === hostCredentialId.value)) {
        hostCredentialId.value = compatible[0].id;
      }
    }
  });

  watch(hostSourceType, (nextSourceType) => {
    if (['agent', 'local'].includes(nextSourceType)) hostAddress.value = '';
    if (['agent', 'local'].includes(nextSourceType)) {
      hostCredentialId.value = '';
      return;
    }
    const firstCredential = credentialsForSource(nextSourceType)[0];
    hostCredentialId.value = firstCredential?.id ?? '';
  });

  async function refresh() {
    if (!authenticated.value) return;
    loading.value = true;
    error.value = '';
    try {
      const [nextHealth, nextBootstrap, nextStorage, maintenanceRunResp, hostResp, credentialResp, jobResp, runResp, snapshotResp, restoreTaskResp] = await Promise.all([
        api.health(),
        api.bootstrap(),
        api.storageHealth(),
        api.maintenanceRuns(),
        api.hosts(),
        api.credentials(),
        api.jobs(),
        api.runs(),
        api.snapshots(),
        api.restoreTasks()
      ]);
      const normalizedBootstrap = normalizeBootstrap(nextBootstrap);
      health.value = nextHealth;
      bootstrap.value = normalizedBootstrap;
      hydrateSettingsForm(normalizedBootstrap);
      storageHealth.value = nextStorage;
      maintenanceRuns.value = arrayOrEmpty(maintenanceRunResp.runs);
      hosts.value = arrayOrEmpty(hostResp.hosts);
      credentials.value = arrayOrEmpty(credentialResp.credentials);
      jobs.value = arrayOrEmpty(jobResp.jobs);
      runs.value = arrayOrEmpty(runResp.runs);
      snapshots.value = arrayOrEmpty(snapshotResp.snapshots);
      selectedSnapshotIds.value = selectedSnapshotIds.value.filter((id) => snapshots.value.some((snapshot) => snapshot.id === id));
      if (selectedSnapshot.value && !snapshots.value.some((snapshot) => snapshot.id === selectedSnapshot.value?.id)) {
        selectedSnapshot.value = null;
        snapshotTreeEntries.value = [];
        snapshotTreeManifest.value = null;
        snapshotTreePath.value = '.';
      }
      restoreTasks.value = arrayOrEmpty(restoreTaskResp.tasks);
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
      if (error.value.includes('authentication required')) {
        authenticated.value = false;
        currentUser.value = '';
      }
    } finally {
      loading.value = false;
    }
  }

  async function checkSession() {
    checkingSession.value = true;
    loginError.value = '';
    try {
      const session = await api.session();
      authenticated.value = true;
      currentUser.value = session.user;
      await refresh();
    } catch {
      authenticated.value = false;
      currentUser.value = '';
    } finally {
      checkingSession.value = false;
    }
  }

  async function login() {
    loginLoading.value = true;
    loginError.value = '';
    try {
      const session = await api.login({
        username: loginUsername.value.trim(),
        password: loginPassword.value
      });
      authenticated.value = true;
      currentUser.value = session.user;
      loginPassword.value = '';
      await refresh();
    } catch (err) {
      loginError.value = err instanceof Error ? err.message : String(err);
    } finally {
      loginLoading.value = false;
    }
  }

  async function logout() {
    error.value = '';
    try {
      await api.logout();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      authenticated.value = false;
      currentUser.value = '';
      navigatePage('dashboard', true);
    }
  }

  function formatTime(value?: string | null) {
    if (!value) return '-';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '-';
    return new Intl.DateTimeFormat(locale.value === 'zh' ? 'zh-CN' : 'en-US', {
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    }).format(date);
  }

  function formatBytes(value: number) {
    if (!Number.isFinite(value)) return '-';
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let size = value;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
      size /= 1024;
      unit += 1;
    }
    return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
  }

  function formatPercent(value: number) {
    if (!Number.isFinite(value)) return '-';
    return `${(value * 100).toFixed(2)}%`;
  }

  function runProgressText(run: Run) {
    const progress = run.progress;
    if (!progress) return '-';
    const filePart =
      progress.total_files > 0
        ? `${progress.processed_files}/${progress.total_files} ${t('unit.files')}`
        : `${progress.processed_files} ${t('unit.files')}`;
    const bytePart =
      progress.total_bytes > 0
        ? `${formatPercent(progress.processed_bytes / progress.total_bytes)}`
        : formatBytes(progress.processed_bytes);
    return `${phaseText(progress.phase)} · ${filePart} · ${bytePart}`;
  }

  function nullText(value: unknown) {
    if (typeof value === 'string' && value !== '') return value;
    if (value && typeof value === 'object') {
      const nullable = value as { String?: unknown; Time?: unknown; Int64?: unknown; Valid?: unknown };
      if (typeof nullable.String === 'string') return nullable.Valid === false ? '-' : nullable.String;
      if (typeof nullable.Time === 'string') return nullable.Valid === false ? '-' : formatTime(nullable.Time);
      if (typeof nullable.Int64 === 'number') return nullable.Valid === false ? '-' : String(nullable.Int64);
    }
    return '-';
  }

  function nullableNumber(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (value && typeof value === 'object') {
      const nullable = value as { Int64?: unknown; Valid?: unknown };
      if (nullable.Valid === false) return null;
      if (typeof nullable.Int64 === 'number' && Number.isFinite(nullable.Int64)) return nullable.Int64;
    }
    return null;
  }

  function selectHost(host: Host) {
    selectedHostId.value = host.id;
    updateHostLocation();
  }

  function setActiveHostTab(tab: HostTab) {
    activeHostTab.value = tab;
    updateHostLocation();
  }

  function selectCredential(credential: Credential) {
    selectedCredentialId.value = credential.id;
    copyActionMessage.value = '';
  }

  function openHostCreate(sourceType = hostSourceType.value) {
    hostSourceType.value = sourceType;
    showHostCreate.value = true;
    hostActionMessage.value = '';
    copyActionMessage.value = '';
  }

  function openCredentialCreate(sourceType = credentialType.value) {
    credentialType.value = ['agent', 'local'].includes(sourceType) ? 'sftp' : sourceType;
    showCredentialCreate.value = true;
    actionMessage.value = '';
    copyActionMessage.value = '';
    agentCredentialClientId.value = '';
    agentCredentialSecret.value = '';
  }

  function hostAddressText(host: Host) {
    const address = nullText(host.address);
    if (host.source_type === 'agent') return address === '-' ? t('hosts.waitingHeartbeat') : address;
    if (host.source_type === 'local') return address === '-' ? t('hosts.localServer') : address;
    return address;
  }

  function hostLastSeenText(host: Host) {
    const lastSeen = nullText(host.last_seen_at);
    return lastSeen === '-' ? t('hosts.waitingHeartbeat') : lastSeen;
  }

  function hostAddressPlaceholder(sourceType: string) {
    if (sourceType === 'sftp') return 'backup.example.com:22';
    if (sourceType === 'ftp' || sourceType === 'ftps') return 'ftp.example.com:21';
    if (sourceType === 'webdav') return 'https://storage.example.com/dav';
    if (sourceType === 'local') return '/mnt/source';
    return t('placeholder.hostAddress');
  }

  async function copyText(value: string) {
    error.value = '';
    copyActionMessage.value = '';
    try {
      if (!navigator.clipboard) throw new Error('clipboard unavailable');
      await navigator.clipboard.writeText(value);
      copyActionMessage.value = t('message.copied');
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    }
  }

  function nonNegativeInt(value: unknown) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) return 0;
    return Math.floor(parsed);
  }

  function positiveInt(value: unknown, fallback = 1) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.floor(parsed);
  }

  function yesNo(value: boolean) {
    return value ? t('common.yes') : t('common.no');
  }

  function statusText(status?: string | null) {
    if (!status) return t('status.loading');
    const key = `status.${status}` as MessageKey;
    return key in en ? t(key) : status;
  }

  function sourceTypeLabel(sourceType: string) {
    const key = `source.${sourceType}` as MessageKey;
    return key in en ? t(key) : sourceType;
  }

  function credentialsForSource(sourceType: string) {
    return credentials.value.filter((credential) => credential.type === sourceType);
  }

  function credentialForHost(host: Host) {
    const credentialId = nullableNumber(host.credential_id);
    if (credentialId === null) return null;
    return credentials.value.find((credential) => credential.id === credentialId) ?? host.credential ?? null;
  }

  function hostsForCredential(credential: Credential) {
    return hosts.value.filter((host) => nullableNumber(host.credential_id) === credential.id);
  }

  function jobsForCredential(credential: Credential) {
    const hostIds = new Set(hostsForCredential(credential).map((host) => host.id));
    return jobs.value.filter((job) => {
      const hostId = nullableNumber(job.host_id);
      return hostId !== null && hostIds.has(hostId);
    });
  }

  function hostForJob(job: Job) {
    const hostId = nullableNumber(job.host_id);
    if (hostId === null) return null;
    return hosts.value.find((host) => host.id === hostId) ?? null;
  }

  function jobHostLabel(job: Job) {
    const host = hostForJob(job);
    return host ? host.name : '-';
  }

  function credentialUsageText(credential: Credential) {
    const linkedHosts = hostsForCredential(credential).length;
    const linkedJobs = jobsForCredential(credential).length;
    return `${linkedJobs} ${t('nav.jobs')} · ${linkedHosts} ${t('nav.hosts')}`;
  }

  function entryTypeLabel(entryType: string) {
    const key = `entry.${entryType}` as MessageKey;
    return key in en ? t(key) : entryType;
  }

  function phaseText(phase: string) {
    const key = `phase.${phase}` as MessageKey;
    return key in en ? t(key) : phase;
  }

  function maintenanceModeText(mode?: string | null) {
    if (!mode) return '-';
    const key = `maintenance.${mode}` as MessageKey;
    return key in en ? t(key) : mode;
  }

  function compactSkippedText(reason?: string | null) {
    if (!reason) return '-';
    if (reason === 'active runs exist') return t('message.activeRunsExist');
    if (reason === 'backup or chunk upload in progress') return t('message.backupWriteBusy');
    if (reason === 'active manifest errors exist') return t('message.activeManifestErrors');
    if (reason === 'compact reclaim threshold not met') return t('message.compactThresholdNotMet');
    return reason;
  }

  function hydrateSettingsForm(nextBootstrap: Bootstrap) {
    settingsAdminUsername.value = nextBootstrap.auth.username;
    settingsSessionTTLHours.value = positiveInt(nextBootstrap.auth.session_ttl_hours, 24);
    settingsKeepLast.value = positiveInt(nextBootstrap.retention.keep_last, 30);
    settingsKeepDaily.value = nonNegativeInt(nextBootstrap.retention.keep_daily);
    settingsKeepWeekly.value = nonNegativeInt(nextBootstrap.retention.keep_weekly);
    settingsMaintenanceEnabled.value = nextBootstrap.maintenance.enabled;
    settingsMaintenanceTimezone.value = nextBootstrap.maintenance.timezone || 'Asia/Shanghai';
    settingsCleanupSchedule.value = nextBootstrap.maintenance.cleanup_schedule || '0 3 * * *';
    settingsCompactEnabled.value = nextBootstrap.maintenance.compact_enabled;
    settingsCompactSchedule.value = nextBootstrap.maintenance.compact_schedule || '30 3 * * 0';
    settingsErrorGracePeriod.value = nextBootstrap.maintenance.error_grace_period || '24h';
    settingsStaleRunAfter.value = nextBootstrap.maintenance.stale_run_after || '6h';
    settingsKeepDeletedMetadataDays.value = nonNegativeInt(nextBootstrap.maintenance.keep_deleted_metadata_days);
    settingsCompactMinReclaimRatio.value = Number.isFinite(nextBootstrap.maintenance.compact_min_reclaim_ratio)
      ? nextBootstrap.maintenance.compact_min_reclaim_ratio
      : 0.15;
    settingsCompactMinReclaimBytes.value = nextBootstrap.maintenance.compact_min_reclaim_bytes || '1GiB';
  }

  function sourceRoot(job: Job) {
    try {
      const parsed = JSON.parse(job.source_config) as { root?: unknown; roots?: unknown; path?: unknown };
      if (Array.isArray(parsed.roots) && parsed.roots.length > 0) {
        return parsed.roots.filter((root): root is string => typeof root === 'string').join(', ');
      }
      if (typeof parsed.root === 'string') return parsed.root;
      if (typeof parsed.path === 'string') return parsed.path;
    } catch {
      return '-';
    }
    return '-';
  }

  function startEditJob(job: Job) {
    const root = sourceRoot(job);
    const schedule = nullText(job.schedule);
    editJobId.value = job.id;
    editJobName.value = job.name;
    editJobRoot.value = root === '-' ? '' : root;
    editJobSchedule.value = schedule === '-' ? '' : schedule;
    editJobTimezone.value = job.timezone || 'Asia/Shanghai';
    editJobMaxRuntimeSeconds.value = nonNegativeInt(job.max_runtime_seconds);
    editJobRetryAttempts.value = nonNegativeInt(job.retry_attempts);
    editJobEnabled.value = job.enabled;
  }

  function cancelEditJob() {
    editJobId.value = null;
    editJobName.value = '';
    editJobRoot.value = '';
    editJobSchedule.value = '';
    editJobTimezone.value = 'Asia/Shanghai';
    editJobMaxRuntimeSeconds.value = 0;
    editJobRetryAttempts.value = 0;
    editJobEnabled.value = true;
  }

  function parentTreePath(path: string) {
    if (!path || path === '.') return '.';
    const parts = path.split('/').filter(Boolean);
    parts.pop();
    return parts.length === 0 ? '.' : parts.join('/');
  }

  function snapshotDownloadURL(snapshot: Snapshot, entryPath = '.') {
    return api.snapshotDownloadURL(snapshot.id, entryPath);
  }

  async function browseSnapshot(snapshot: Snapshot, path = '.') {
    error.value = '';
    loadingTree.value = true;
    try {
      const tree = await api.snapshotTree(snapshot.id, path);
      selectedSnapshot.value = tree.snapshot;
      snapshotTreePath.value = tree.path;
      snapshotTreeEntries.value = tree.entries;
      snapshotTreeManifest.value = tree.manifest;
      restoreSnapshotId.value = tree.snapshot.id;
      restoreEntryPath.value = tree.path;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loadingTree.value = false;
    }
  }

  function selectRestore(snapshot: Snapshot, path = '.') {
    restoreSnapshotId.value = snapshot.id;
    restoreEntryPath.value = path;
    navigatePage('restore');
  }

  function toggleSnapshotSelection(snapshot: Snapshot, selected: boolean) {
    if (selected) {
      if (!selectedSnapshotIds.value.includes(snapshot.id)) selectedSnapshotIds.value = [...selectedSnapshotIds.value, snapshot.id];
      return;
    }
    selectedSnapshotIds.value = selectedSnapshotIds.value.filter((id) => id !== snapshot.id);
  }

  function toggleAllSnapshots(selected: boolean) {
    selectedSnapshotIds.value = selected ? snapshots.value.map((snapshot) => snapshot.id) : [];
  }

  function requestDeleteSnapshot(snapshot: Snapshot) {
    snapshotDeleteTarget.value = snapshot;
    snapshotDeleteMany.value = false;
  }

  function requestDeleteSelectedSnapshots() {
    if (selectedSnapshotIds.value.length === 0) return;
    snapshotDeleteTarget.value = null;
    snapshotDeleteMany.value = true;
  }

  function cancelSnapshotDelete() {
    snapshotDeleteTarget.value = null;
    snapshotDeleteMany.value = false;
  }

  async function confirmSnapshotDelete() {
    error.value = '';
    actionMessage.value = '';
    deletingSnapshots.value = true;
    try {
      if (snapshotDeleteMany.value) {
        const ids = [...selectedSnapshotIds.value];
        const result = await api.deleteSnapshots(ids);
        const failed = result.results.filter((item) => item.status === 'error');
        selectedSnapshotIds.value = [];
        actionMessage.value =
          failed.length > 0
            ? t('message.snapshotsDeletedPartial', { count: result.results.length - failed.length, failed: failed.length })
            : t('message.snapshotsDeleted', { count: result.results.length });
      } else if (snapshotDeleteTarget.value) {
        const target = snapshotDeleteTarget.value;
        const result = await api.deleteSnapshot(target.id);
        selectedSnapshotIds.value = selectedSnapshotIds.value.filter((id) => id !== target.id);
        actionMessage.value = result.deleted ? t('message.snapshotDeleted', { id: target.id }) : t('message.snapshotAlreadyDeleted', { id: target.id });
      }
      cancelSnapshotDelete();
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      deletingSnapshots.value = false;
    }
  }

  async function createCredential() {
    error.value = '';
    actionMessage.value = '';
    agentCredentialClientId.value = '';
    agentCredentialSecret.value = '';
    try {
      const createdType = credentialType.value;
      const createdName = credentialName.value.trim();
      const payload: Record<string, unknown> = {};
      if (createdType === 'webdav') {
        payload.username = credentialUsername.value.trim();
        payload.password = credentialPassword.value;
        payload.bearer_token = credentialBearerToken.value.trim();
      } else {
        payload.username = credentialUsername.value.trim();
        payload.password = credentialPassword.value;
        payload.private_key = credentialPrivateKey.value;
        if (createdType === 'ftps') {
          payload.tls = true;
          payload.explicit_tls = credentialExplicitTLS.value;
          payload.skip_tls_verify = credentialSkipTLSVerify.value;
        }
      }
      const result = await api.createCredential({
        name: createdName,
        type: createdType,
        payload
      });
      selectedCredentialId.value = result.credential.id;
      actionMessage.value = t('message.credentialCreated');
      showCredentialCreate.value = false;
      credentialName.value = '';
      credentialUsername.value = '';
      credentialPassword.value = '';
      credentialPrivateKey.value = '';
      credentialBearerToken.value = '';
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    }
  }

  async function createHost() {
    error.value = '';
    actionMessage.value = '';
    hostActionMessage.value = '';
    agentCredentialClientId.value = '';
    agentCredentialSecret.value = '';
    copyActionMessage.value = '';
    try {
      const name = hostName.value.trim();
      const sourceType = hostSourceType.value;
      const payload: { name: string; source_type: string; address?: string; credential_id?: number } = {
        name,
        source_type: sourceType
      };
      if (!['agent', 'local'].includes(sourceType)) {
        payload.address = hostAddress.value.trim();
      }
      if (!['agent', 'local'].includes(sourceType)) {
        payload.credential_id = Number(hostCredentialId.value);
      }
      const created = await api.createHost(payload);
      selectedHostId.value = created.host.id;
      if (sourceType === 'agent') {
        activeHostTab.value = 'access';
        agentSetupHostName.value = name;
        agentCredentialClientId.value = created.agent?.client_id ?? created.host.agent?.client_id ?? '';
        agentCredentialSecret.value = created.agent?.client_secret ?? created.host.agent?.client_secret ?? '';
        actionMessage.value = t('message.agentClientCreated');
        hostActionMessage.value = t('message.agentClientCreated');
      } else {
        activeHostTab.value = 'overview';
        actionMessage.value = t('message.hostCreated');
        hostActionMessage.value = t('message.hostCreated');
      }
      showHostCreate.value = false;
      hostName.value = '';
      hostAddress.value = '';
      hostCredentialId.value = '';
      updateHostLocation(true);
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    }
  }

  async function createJob() {
    error.value = '';
    actionMessage.value = '';
    jobActionMessage.value = '';
    try {
      const payload: {
        name: string;
        host_id: number;
        source_config: Record<string, unknown>;
        enabled: boolean;
        schedule?: string;
        timezone?: string;
        max_runtime_seconds: number;
        retry_attempts: number;
      } = {
        name: jobName.value.trim(),
        host_id: Number(jobHostId.value),
        source_config: { root: jobRoot.value.trim() },
        enabled: true,
        max_runtime_seconds: nonNegativeInt(jobMaxRuntimeSeconds.value),
        retry_attempts: nonNegativeInt(jobRetryAttempts.value)
      };
      if (jobSchedule.value.trim() !== '') {
        payload.schedule = jobSchedule.value.trim();
        payload.timezone = jobTimezone.value.trim() || 'Asia/Shanghai';
      }
      await api.createJob(payload);
      actionMessage.value = t('message.jobCreated');
      jobActionMessage.value = t('message.jobCreated');
      showJobCreate.value = false;
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    }
  }

  async function saveJob(job: Job) {
    error.value = '';
    actionMessage.value = '';
    savingJobId.value = job.id;
    try {
      await api.updateJob(job.id, {
        name: editJobName.value.trim(),
        source_config: { root: editJobRoot.value.trim() },
        enabled: editJobEnabled.value,
        schedule: editJobSchedule.value.trim(),
        timezone: editJobTimezone.value.trim() || 'Asia/Shanghai',
        max_runtime_seconds: nonNegativeInt(editJobMaxRuntimeSeconds.value),
        retry_attempts: nonNegativeInt(editJobRetryAttempts.value)
      });
      actionMessage.value = t('message.jobUpdated', { id: job.id });
      cancelEditJob();
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      savingJobId.value = null;
    }
  }

  async function toggleJob(job: Job) {
    error.value = '';
    actionMessage.value = '';
    savingJobId.value = job.id;
    try {
      const result = await api.updateJob(job.id, { enabled: !job.enabled });
      actionMessage.value = t('message.jobToggled', {
        id: result.job.id,
        status: result.job.enabled ? t('message.jobEnabled') : t('message.jobDisabled')
      });
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      savingJobId.value = null;
    }
  }

  async function runMaintenance(mode = 'retention') {
    error.value = '';
    actionMessage.value = '';
    maintenanceRunning.value = true;
    try {
      maintenanceReport.value = await api.storageMaintenance(mode);
      if (mode === 'verify') {
        actionMessage.value = t('message.verifiedChunks', { count: maintenanceReport.value.verify.verified_chunks });
      } else if (mode === 'compact' || mode === 'full-cleanup') {
        actionMessage.value =
          compactSkippedText(maintenanceReport.value.compact.skipped_reason) !== '-'
            ? compactSkippedText(maintenanceReport.value.compact.skipped_reason)
            : t('message.compactedChunks', { count: maintenanceReport.value.compact.rewritten_chunks });
      } else if (mode === 'cleanup-errors') {
        actionMessage.value = t('message.cleanupRemoved', {
          chunks: maintenanceReport.value.cleanup.removed_chunks,
          manifests: maintenanceReport.value.cleanup.removed_manifests
        });
      } else {
        actionMessage.value = t('message.maintenanceExpired', { count: maintenanceReport.value.retention.expired_snapshots });
      }
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      maintenanceRunning.value = false;
    }
  }

  async function saveSettings() {
    error.value = '';
    actionMessage.value = '';
    savingSettings.value = true;
    try {
      const payload: {
        admin_username: string;
        current_password?: string;
        admin_password?: string;
        session_ttl_hours: number;
        retention: {
          keep_last: number;
          keep_daily: number;
          keep_weekly: number;
        };
        maintenance: MaintenanceSettings;
      } = {
        admin_username: settingsAdminUsername.value.trim(),
        session_ttl_hours: positiveInt(settingsSessionTTLHours.value, 24),
        retention: {
          keep_last: positiveInt(settingsKeepLast.value, 30),
          keep_daily: nonNegativeInt(settingsKeepDaily.value),
          keep_weekly: nonNegativeInt(settingsKeepWeekly.value)
        },
        maintenance: {
          enabled: settingsMaintenanceEnabled.value,
          timezone: settingsMaintenanceTimezone.value.trim() || 'Asia/Shanghai',
          cleanup_schedule: settingsCleanupSchedule.value.trim() || '0 3 * * *',
          compact_enabled: settingsCompactEnabled.value,
          compact_schedule: settingsCompactSchedule.value.trim() || '30 3 * * 0',
          error_grace_period: settingsErrorGracePeriod.value.trim() || '24h',
          stale_run_after: settingsStaleRunAfter.value.trim() || '6h',
          keep_deleted_metadata_days: nonNegativeInt(settingsKeepDeletedMetadataDays.value),
          compact_min_reclaim_ratio: Number(settingsCompactMinReclaimRatio.value),
          compact_min_reclaim_bytes: settingsCompactMinReclaimBytes.value.trim() || '1GiB'
        }
      };
      if (settingsNewPassword.value !== '') {
        payload.current_password = settingsCurrentPassword.value;
        payload.admin_password = settingsNewPassword.value;
      }
      await api.updateSettings(payload);
      settingsCurrentPassword.value = '';
      settingsNewPassword.value = '';
      actionMessage.value = t('message.settingsSaved');
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      savingSettings.value = false;
    }
  }

  async function restorePath() {
    error.value = '';
    actionMessage.value = '';
    restoreResult.value = null;
    restoring.value = true;
    try {
      restoreResult.value = await api.restore({
        snapshot_id: Number(restoreSnapshotId.value),
        path: restoreEntryPath.value.trim() || '.',
        target_path: restoreTargetPath.value.trim()
      });
      actionMessage.value = t('message.restoreStatus', {
        id: restoreResult.value.task.id,
        status: statusText(restoreResult.value.status)
      });
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      restoring.value = false;
    }
  }

  async function runJob(job: Job) {
    error.value = '';
    actionMessage.value = '';
    runningJobId.value = job.id;
    try {
      const result = await api.runJob(job.id);
      if (result.run) {
        actionMessage.value = t('message.runStatus', { id: result.run.id, status: statusText(result.status) });
      } else if (result.command) {
        actionMessage.value = t('message.commandStatus', { id: result.command.id, status: statusText(result.status) });
      } else {
        actionMessage.value = statusText(result.status);
      }
      await refresh();
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      runningJobId.value = null;
    }
  }

  async function viewRunLogs(run: Run) {
    error.value = '';
    selectedRunId.value = run.id;
    loadingRunLogs.value = true;
    try {
      const response = await api.runLogs(run.id);
      runLogs.value = response.logs;
    } catch (err) {
      error.value = err instanceof Error ? err.message : String(err);
    } finally {
      loadingRunLogs.value = false;
    }
  }

  onMounted(() => {
    if (typeof window !== 'undefined') {
      syncPageFromLocation();
      window.addEventListener('popstate', syncPageFromLocation);
    }
    void checkSession();
  });

  onBeforeUnmount(() => {
    if (typeof window !== 'undefined') {
      window.removeEventListener('popstate', syncPageFromLocation);
    }
    if (agentSetupSaveTimer) {
      clearTimeout(agentSetupSaveTimer);
      agentSetupSaveTimer = null;
    }
  });

  return {
    t,
    locale,
    setLocale,
    pages,
    activePage,
    activeTitle,
    pagePath,
    navigatePage,
    loading,
    error,
    checkingSession,
    authenticated,
    currentUser,
    loginUsername,
    loginPassword,
    loginLoading,
    loginError,
    health,
    bootstrap,
    storageHealth,
    maintenanceReport,
    hosts,
    credentials,
    jobs,
    runs,
    snapshots,
    restoreTasks,
    maintenanceRuns,
    selectedSnapshot,
    selectedSnapshotIds,
    snapshotDeleteTarget,
    snapshotDeleteMany,
    deletingSnapshots,
    snapshotTreePath,
    snapshotTreeEntries,
    snapshotTreeManifest,
    hostName,
    hostSourceType,
    hostAddress,
    hostCredentialId,
    showHostCreate,
    hostActionMessage,
    selectedHostId,
    activeHostTab,
    hostSearch,
    hostSourceFilter,
    agentSetupHostName,
    agentSetupSourceDir,
    agentSetupSourceDirs,
    agentSetupBackupSchedule,
    agentSetupScheduleMode,
    agentSetupScheduleMinute,
    agentSetupScheduleHour,
    agentSetupScheduleWeekday,
    agentSetupSaveMessage,
    savingAgentSetup,
    agentSetupRunMode,
    agentSetupMethod,
    copyActionMessage,
    credentialName,
    credentialType,
    credentialUsername,
    credentialPassword,
    credentialPrivateKey,
    credentialBearerToken,
    credentialExplicitTLS,
    credentialSkipTLSVerify,
    showCredentialCreate,
    selectedCredentialId,
    agentCredentialClientId,
    agentCredentialSecret,
    jobName,
    jobRoot,
    jobHostId,
    jobSchedule,
    jobTimezone,
    jobMaxRuntimeSeconds,
    jobRetryAttempts,
    showJobCreate,
    jobActionMessage,
    restoreSnapshotId,
    restoreEntryPath,
    restoreTargetPath,
    restoreResult,
    runningJobId,
    loadingTree,
    restoring,
    maintenanceRunning,
    actionMessage,
    editJobId,
    editJobName,
    editJobRoot,
    editJobSchedule,
    editJobTimezone,
    editJobMaxRuntimeSeconds,
    editJobRetryAttempts,
    editJobEnabled,
    savingJobId,
    selectedRunId,
    runLogs,
    loadingRunLogs,
    settingsAdminUsername,
    settingsSessionTTLHours,
    settingsCurrentPassword,
    settingsNewPassword,
    settingsKeepLast,
    settingsKeepDaily,
    settingsKeepWeekly,
    settingsMaintenanceEnabled,
    settingsMaintenanceTimezone,
    settingsCleanupSchedule,
    settingsCompactEnabled,
    settingsCompactSchedule,
    settingsErrorGracePeriod,
    settingsStaleRunAfter,
    settingsKeepDeletedMetadataDays,
    settingsCompactMinReclaimRatio,
    settingsCompactMinReclaimBytes,
    savingSettings,
    counts,
    statRows,
    hostSummaryRows,
    hostSourceOptions,
    hostSourceFilterOptions,
    filteredHosts,
    hostTabOptions,
    credentialSourceOptions,
    credentialSummaryRows,
    jobHostOptions,
    selectedHost,
    selectedHostCredentials,
    selectedHostJobs,
    selectedAgentCredential,
    selectedCredential,
    selectedCredentialJobs,
    selectedCredentialHosts,
    currentServerURL,
    agentSetupClientId,
    agentSetupClientSecret,
    agentContainerStateDir,
    agentSetupRoots,
    agentSetupRootErrors,
    agentSetupScheduleErrors,
    agentSetupScheduleModeOptions,
    agentScheduleMinuteOptions,
    agentScheduleHourOptions,
    agentSetupScheduleWeekdayOptions,
    agentSetupReady,
    agentRunModeOptions,
    agentSetupStateMount,
    agentSetupSourceMounts,
    agentSetupSourceMount,
    agentComposeEnv,
    agentComposeSetup,
    agentDockerCommand,
    agentBinaryCommand,
    agentSystemdTimer,
    agentSetupMethods,
    selectedAgentSetupSnippet,
    addAgentSetupSourceDir,
    removeAgentSetupSourceDir,
    refresh,
    checkSession,
    login,
    logout,
    formatTime,
    formatBytes,
    formatPercent,
    runProgressText,
    nullText,
    nullableNumber,
    selectHost,
    setActiveHostTab,
    selectCredential,
    openHostCreate,
    openCredentialCreate,
    hostAddressText,
    hostLastSeenText,
    hostAddressPlaceholder,
    copyText,
    nonNegativeInt,
    positiveInt,
    yesNo,
    statusText,
    sourceTypeLabel,
    credentialsForSource,
    credentialForHost,
    jobHostLabel,
    credentialUsageText,
    entryTypeLabel,
    phaseText,
    maintenanceModeText,
    compactSkippedText,
    hydrateSettingsForm,
    sourceRoot,
    startEditJob,
    cancelEditJob,
    parentTreePath,
    snapshotDownloadURL,
    browseSnapshot,
    selectRestore,
    toggleSnapshotSelection,
    toggleAllSnapshots,
    requestDeleteSnapshot,
    requestDeleteSelectedSnapshots,
    cancelSnapshotDelete,
    confirmSnapshotDelete,
    createCredential,
    createHost,
    createJob,
    saveJob,
    toggleJob,
    runMaintenance,
    saveSettings,
    restorePath,
    runJob,
    viewRunLogs
  };
}
