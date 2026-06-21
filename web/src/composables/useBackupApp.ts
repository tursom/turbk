import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { Activity, Archive, CalendarClock, Database, HardDrive, History, KeyRound, Server, Shield, UploadCloud } from '@lucide/vue';
import {
  api,
  type Bootstrap,
  type Credential,
  type Health,
  type Host,
  type Job,
  type RestoreResult,
  type RestoreTask,
  type Run,
  type RunLog,
  type Snapshot,
  type SnapshotTreeEntry,
  type StorageHealth,
  type StorageMaintenance
} from '../api';
import { en, locale, setLocale, t, type MessageKey } from '../i18n';
import { pages, type PageKey } from '../navigation';

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
  const selectedSnapshot = ref<Snapshot | null>(null);
  const snapshotTreePath = ref('.');
  const snapshotTreeEntries = ref<SnapshotTreeEntry[]>([]);
  const hostName = ref('');
  const hostSourceType = ref('local');
  const hostAddress = ref('');
  const hostCredentialId = ref<number | ''>('');
  const showHostCreate = ref(false);
  const hostActionMessage = ref('');
  const selectedHostId = ref<number | null>(null);
  const agentSetupHostName = ref('');
  const agentSetupSourceDir = ref('/srv/data');
  const agentSetupRoot = ref('/backup/source');
  const agentSetupJobName = ref('');
  const copyActionMessage = ref('');
  const credentialName = ref('');
  const credentialType = ref('sftp');
  const credentialAddress = ref('');
  const credentialUsername = ref('');
  const credentialPassword = ref('');
  const credentialPrivateKey = ref('');
  const credentialBearerToken = ref('');
  const credentialSubject = ref('');
  const credentialExplicitTLS = ref(true);
  const credentialSkipTLSVerify = ref(false);
  const showCredentialCreate = ref(false);
  const selectedCredentialId = ref<number | null>(null);
  const agentCredentialClientId = ref('');
  const agentCredentialSecret = ref('');
  const jobName = ref('');
  const jobSourceType = ref('local');
  const jobRoot = ref('');
  const jobHostId = ref<number | ''>('');
  const jobCredentialId = ref<number | ''>('');
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
  const savingSettings = ref(false);

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
      }
    };
  }

  function navigatePage(page: PageKey, replace = false) {
    activePage.value = page;
    if (typeof window === 'undefined') return;
    const nextPath = pagePath(page);
    if (window.location.pathname === nextPath) return;
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
  const agentSetupDisplayName = computed(() => agentSetupHostName.value || selectedHost.value?.name || activeAgentCredential.value?.subject || 'agent-source');
  const jobHostOptions = computed(() => hosts.value.filter((host) => host.source_type !== 'agent'));
  const effectiveAgentSetupJobName = computed(() => agentSetupJobName.value.trim() || `${agentSetupDisplayName.value}-backup`);

  const agentComposeEnv = computed(() =>
    [
      `TURBK_SERVER_URL=${currentServerURL.value}`,
      `TURBK_AGENT_ID=${agentSetupClientId.value}`,
      `TURBK_AGENT_SECRET=${agentSetupClientSecret.value}`,
      `TURBK_AGENT_SOURCE_DIR=${agentSetupSourceDir.value}`,
      `TURBK_AGENT_ROOT=${agentSetupRoot.value}`,
      `TURBK_AGENT_JOB_NAME=${effectiveAgentSetupJobName.value}`
    ].join('\n')
  );

  const agentDockerCommand = computed(() =>
    [
      'docker run --rm \\',
      `  -e TURBK_SERVER_URL=${JSON.stringify(currentServerURL.value)} \\`,
      `  -e TURBK_AGENT_ID=${JSON.stringify(agentSetupClientId.value)} \\`,
      `  -e TURBK_AGENT_SECRET=${JSON.stringify(agentSetupClientSecret.value)} \\`,
      `  -e TURBK_AGENT_ROOT=${JSON.stringify(agentSetupRoot.value)} \\`,
      `  -e TURBK_AGENT_JOB_NAME=${JSON.stringify(effectiveAgentSetupJobName.value)} \\`,
      `  -v ${JSON.stringify(`${agentSetupSourceDir.value}:${agentSetupRoot.value}:ro`)} \\`,
      '  ghcr.io/tursom/turbk-agent:latest \\',
      '  -once'
    ].join('\n')
  );

  watch(hosts, (nextHosts) => {
    if (nextHosts.length === 0) {
      selectedHostId.value = null;
      jobHostId.value = '';
      return;
    }
    if (selectedHostId.value === null || !nextHosts.some((host) => host.id === selectedHostId.value)) {
      selectedHostId.value = nextHosts[0].id;
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
    if (nextSourceType === 'agent') hostAddress.value = '';
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
      const [nextHealth, nextBootstrap, nextStorage, hostResp, credentialResp, jobResp, runResp, snapshotResp, restoreTaskResp] = await Promise.all([
        api.health(),
        api.bootstrap(),
        api.storageHealth(),
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
      hosts.value = arrayOrEmpty(hostResp.hosts);
      credentials.value = arrayOrEmpty(credentialResp.credentials);
      jobs.value = arrayOrEmpty(jobResp.jobs);
      runs.value = arrayOrEmpty(runResp.runs);
      snapshots.value = arrayOrEmpty(snapshotResp.snapshots);
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
    return reason;
  }

  function hydrateSettingsForm(nextBootstrap: Bootstrap) {
    settingsAdminUsername.value = nextBootstrap.auth.username;
    settingsSessionTTLHours.value = positiveInt(nextBootstrap.auth.session_ttl_hours, 24);
    settingsKeepLast.value = positiveInt(nextBootstrap.retention.keep_last, 30);
    settingsKeepDaily.value = nonNegativeInt(nextBootstrap.retention.keep_daily);
    settingsKeepWeekly.value = nonNegativeInt(nextBootstrap.retention.keep_weekly);
  }

  function sourceRoot(job: Job) {
    try {
      const parsed = JSON.parse(job.source_config) as { root?: unknown; path?: unknown };
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
      credentialAddress.value = '';
      credentialUsername.value = '';
      credentialPassword.value = '';
      credentialPrivateKey.value = '';
      credentialBearerToken.value = '';
      credentialSubject.value = '';
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
      if (sourceType !== 'agent') {
        payload.address = hostAddress.value.trim();
      }
      if (!['agent', 'local'].includes(sourceType)) {
        payload.credential_id = Number(hostCredentialId.value);
      }
      const created = await api.createHost(payload);
      selectedHostId.value = created.host.id;
      if (sourceType === 'agent') {
        agentSetupHostName.value = name;
        if (agentSetupJobName.value.trim() === '') agentSetupJobName.value = `${name}-backup`;
        agentCredentialClientId.value = created.agent?.client_id ?? created.host.agent?.client_id ?? '';
        agentCredentialSecret.value = created.agent?.client_secret ?? created.host.agent?.client_secret ?? '';
        actionMessage.value = t('message.agentClientCreated');
        hostActionMessage.value = t('message.agentClientCreated');
      } else {
        actionMessage.value = t('message.hostCreated');
        hostActionMessage.value = t('message.hostCreated');
      }
      showHostCreate.value = false;
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
      } else if (mode === 'compact') {
        actionMessage.value =
          compactSkippedText(maintenanceReport.value.compact.skipped_reason) !== '-'
            ? compactSkippedText(maintenanceReport.value.compact.skipped_reason)
            : t('message.compactedChunks', { count: maintenanceReport.value.compact.rewritten_chunks });
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
      } = {
        admin_username: settingsAdminUsername.value.trim(),
        session_ttl_hours: positiveInt(settingsSessionTTLHours.value, 24),
        retention: {
          keep_last: positiveInt(settingsKeepLast.value, 30),
          keep_daily: nonNegativeInt(settingsKeepDaily.value),
          keep_weekly: nonNegativeInt(settingsKeepWeekly.value)
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
      actionMessage.value = t('message.runStatus', { id: result.run.id, status: statusText(result.status) });
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
    selectedSnapshot,
    snapshotTreePath,
    snapshotTreeEntries,
    hostName,
    hostSourceType,
    hostAddress,
    hostCredentialId,
    showHostCreate,
    hostActionMessage,
    selectedHostId,
    agentSetupHostName,
    agentSetupSourceDir,
    agentSetupRoot,
    agentSetupJobName,
    copyActionMessage,
    credentialName,
    credentialType,
    credentialAddress,
    credentialUsername,
    credentialPassword,
    credentialPrivateKey,
    credentialBearerToken,
    credentialSubject,
    credentialExplicitTLS,
    credentialSkipTLSVerify,
    showCredentialCreate,
    selectedCredentialId,
    agentCredentialClientId,
    agentCredentialSecret,
    jobName,
    jobSourceType,
    jobRoot,
    jobHostId,
    jobCredentialId,
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
    savingSettings,
    counts,
    statRows,
    hostSummaryRows,
    hostSourceOptions,
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
    agentComposeEnv,
    agentDockerCommand,
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
