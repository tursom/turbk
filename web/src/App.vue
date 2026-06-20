<script setup lang="ts">
import { computed, defineComponent, h, onMounted, ref, watch } from 'vue';
import {
  Activity,
  ArrowUp,
  Archive,
  CalendarClock,
  CheckCircle2,
  Database,
  Download,
  FolderOpen,
  HardDrive,
  History,
  Home,
  KeyRound,
  LogOut,
  Pencil,
  Play,
  Plus,
  Power,
  RefreshCw,
  RotateCcw,
  Save,
  ScrollText,
  Server,
  Settings,
  Shield,
  UploadCloud,
  User,
  X,
  Wrench
} from '@lucide/vue';
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
} from './api';

const en = {
  'language': 'Language',
  'language.zh': '中文',
  'language.en': 'EN',
  'nav.dashboard': 'Dashboard',
  'nav.hosts': 'Hosts',
  'nav.credentials': 'Credentials',
  'nav.jobs': 'Jobs',
  'nav.runs': 'Runs',
  'nav.snapshots': 'Snapshots',
  'nav.restore': 'Restore',
  'nav.storage': 'Storage',
  'nav.settings': 'Settings',
  'nav.primary': 'Primary',
  'brand.subtitle': 'Backup Server',
  'common.loading': 'loading',
  'common.refresh': 'Refresh',
  'common.logout': 'Logout',
  'common.close': 'Close',
  'common.create': 'Create',
  'common.save': 'Save',
  'common.saving': 'Saving',
  'common.cancel': 'Cancel',
  'common.run': 'Run',
  'common.running': 'Running',
  'common.edit': 'Edit',
  'common.enable': 'Enable',
  'common.disable': 'Disable',
  'common.browse': 'Browse',
  'common.download': 'Download',
  'common.restore': 'Restore',
  'common.logs': 'Logs',
  'common.open': 'Open',
  'common.up': 'Up',
  'common.select': 'Select',
  'common.noRecords': 'No records',
  'common.yes': 'yes',
  'common.no': 'no',
  'common.agent': 'Agent',
  'common.verify': 'Verify',
  'common.compact': 'Compact',
  'auth.username': 'Username',
  'auth.password': 'Password',
  'auth.signIn': 'Sign in',
  'auth.signingIn': 'Signing in',
  'dashboard.hostsDetail': 'managed sources',
  'dashboard.jobsDetail': 'backup schedules',
  'dashboard.runsDetail': 'execution records',
  'dashboard.snapshotsDetail': 'restore points',
  'dashboard.system': 'System',
  'dashboard.database': 'Database',
  'dashboard.started': 'Started',
  'dashboard.repository': 'Repository',
  'dashboard.state': 'State',
  'dashboard.backupModes': 'Backup Modes',
  'dashboard.agentHTTP': 'Agent HTTP',
  'hosts.new': 'New Host',
  'hosts.create': 'Create Host',
  'hosts.clientCredentials': 'Client Credentials',
  'jobs.new': 'New Job',
  'jobs.create': 'Create Job',
  'credentials.saved': 'Saved Credentials',
  'runs.logsTitle': 'Run #{id} Logs',
  'snapshots.title': 'Snapshot #{id}',
  'restore.roots': 'Restore Roots',
  'restore.tasks': 'Restore Tasks',
  'storage.index': 'Index',
  'storage.maintenance': 'Maintenance',
  'settings.repositoryDefaults': 'Repository Defaults',
  'settings.runtime': 'Runtime Settings',
  'field.id': 'ID',
  'field.name': 'Name',
  'field.source': 'Source',
  'field.address': 'Address',
  'field.status': 'Status',
  'field.updated': 'Updated',
  'field.type': 'Type',
  'field.created': 'Created',
  'field.root': 'Root',
  'field.schedule': 'Schedule',
  'field.timezone': 'Timezone',
  'field.maxRuntime': 'Max Runtime',
  'field.maxRuntimeSeconds': 'Max Runtime (s)',
  'field.retries': 'Retries',
  'field.enabled': 'Enabled',
  'field.action': 'Action',
  'field.credential': 'Credential',
  'field.job': 'Job',
  'field.progress': 'Progress',
  'field.started': 'Started',
  'field.finished': 'Finished',
  'field.error': 'Error',
  'field.manifest': 'Manifest',
  'field.files': 'Files',
  'field.size': 'Size',
  'field.modified': 'Modified',
  'field.snapshot': 'Snapshot',
  'field.path': 'Path',
  'field.target': 'Target',
  'field.task': 'Task',
  'field.mode': 'Mode',
  'field.segmentSize': 'Segment Size',
  'field.segments': 'Segments',
  'field.logical': 'Logical',
  'field.compressed': 'Compressed',
  'field.manifests': 'Manifests',
  'field.retention': 'Retention',
  'field.expired': 'Expired',
  'field.utilization': 'Utilization',
  'field.orphans': 'Orphans',
  'field.verified': 'Verified',
  'field.verifyErrors': 'Verify Errors',
  'field.compacted': 'Compacted',
  'field.compactSkipped': 'Compact Skipped',
  'field.segment': 'Segment',
  'field.chunk': 'Chunk',
  'field.compression': 'Compression',
  'field.adminUsername': 'Admin Username',
  'field.sessionTTL': 'Session TTL (h)',
  'field.currentPassword': 'Current Password',
  'field.newPassword': 'New Password',
  'field.keepLast': 'Keep Last',
  'field.daily': 'Daily',
  'field.weekly': 'Weekly',
  'field.baseURL': 'Base URL',
  'field.bearerToken': 'Bearer Token',
  'field.clientID': 'Client ID',
  'field.clientSecret': 'Client Secret',
  'field.subject': 'Subject',
  'field.privateKey': 'Private Key',
  'field.explicitTLS': 'Explicit TLS',
  'field.skipTLSVerify': 'Skip TLS verify',
  'field.sqlite': 'SQLite',
  'source.local': 'Local',
  'source.sftp': 'SFTP',
  'source.ftp': 'FTP',
  'source.ftps': 'FTPS',
  'source.webdav': 'WebDAV',
  'source.agent': 'Agent',
  'entry.file': 'file',
  'entry.dir': 'directory',
  'phase.scanning': 'scanning',
  'phase.manifest': 'manifest',
  'phase.uploading': 'uploading',
  'phase.running': 'running',
  'phase.completed': 'completed',
  'phase.failed': 'failed',
  'maintenance.retention': 'retention',
  'maintenance.verify': 'verify',
  'maintenance.compact': 'compact',
  'placeholder.hostAddress': 'host:port or label',
  'placeholder.hostLabel': 'host label',
  'message.credentialCreated': 'Credential created',
  'message.agentSecretStored': 'This client secret is stored and can be viewed in Saved Credentials.',
  'message.agentClientCreated': 'Client created and credentials generated',
  'message.hostCreated': 'Host created',
  'message.jobCreated': 'Job created',
  'message.jobUpdated': 'Job #{id} updated',
  'message.jobToggled': 'Job #{id} {status}',
  'message.jobEnabled': 'enabled',
  'message.jobDisabled': 'disabled',
  'message.verifiedChunks': 'Verified {count} chunks',
  'message.compactedChunks': 'Compacted {count} chunks',
  'message.maintenanceExpired': 'Maintenance expired {count} snapshots',
  'message.activeRunsExist': 'active runs exist',
  'message.settingsSaved': 'Settings saved',
  'message.restoreStatus': 'Restore #{id} {status}',
  'message.runStatus': 'Run #{id} {status}',
  'unit.files': 'files',
  'unit.chunks': 'chunks',
  'summary.retention': '{active} active / {deleted} deleted',
  'summary.compacted': '{chunks} chunks / {bytes}',
  'status.ok': 'ok',
  'status.loading': 'loading',
  'status.enabled': 'enabled',
  'status.disabled': 'disabled',
  'status.pending': 'pending',
  'status.running': 'running',
  'status.completed': 'completed',
  'status.failed': 'failed',
  'status.error': 'error',
  'status.accepted': 'accepted',
  'status.unknown': 'unknown',
  'status.online': 'online',
  'status.canceled': 'canceled'
} as const;

const zh: Record<keyof typeof en, string> = {
  'language': '语言',
  'language.zh': '中文',
  'language.en': 'EN',
  'nav.dashboard': '概览',
  'nav.hosts': '主机',
  'nav.credentials': '凭据',
  'nav.jobs': '任务',
  'nav.runs': '运行记录',
  'nav.snapshots': '快照',
  'nav.restore': '恢复',
  'nav.storage': '存储',
  'nav.settings': '设置',
  'nav.primary': '主导航',
  'brand.subtitle': '备份服务器',
  'common.loading': '加载中',
  'common.refresh': '刷新',
  'common.logout': '退出登录',
  'common.close': '关闭',
  'common.create': '创建',
  'common.save': '保存',
  'common.saving': '保存中',
  'common.cancel': '取消',
  'common.run': '运行',
  'common.running': '运行中',
  'common.edit': '编辑',
  'common.enable': '启用',
  'common.disable': '禁用',
  'common.browse': '浏览',
  'common.download': '下载',
  'common.restore': '恢复',
  'common.logs': '日志',
  'common.open': '打开',
  'common.up': '上级',
  'common.select': '选择',
  'common.noRecords': '暂无记录',
  'common.yes': '是',
  'common.no': '否',
  'common.agent': '客户端',
  'common.verify': '校验',
  'common.compact': '压缩整理',
  'auth.username': '用户名',
  'auth.password': '密码',
  'auth.signIn': '登录',
  'auth.signingIn': '登录中',
  'dashboard.hostsDetail': '已管理来源',
  'dashboard.jobsDetail': '备份计划',
  'dashboard.runsDetail': '执行记录',
  'dashboard.snapshotsDetail': '恢复点',
  'dashboard.system': '系统',
  'dashboard.database': '数据库',
  'dashboard.started': '启动时间',
  'dashboard.repository': '仓库',
  'dashboard.state': '状态目录',
  'dashboard.backupModes': '备份模式',
  'dashboard.agentHTTP': '客户端 HTTP',
  'hosts.new': '新建主机',
  'hosts.create': '创建主机',
  'hosts.clientCredentials': '客户端凭据',
  'jobs.new': '新建任务',
  'jobs.create': '创建任务',
  'credentials.saved': '已保存凭据',
  'runs.logsTitle': '运行 #{id} 日志',
  'snapshots.title': '快照 #{id}',
  'restore.roots': '恢复根目录',
  'restore.tasks': '恢复任务',
  'storage.index': '索引',
  'storage.maintenance': '维护',
  'settings.repositoryDefaults': '仓库默认值',
  'settings.runtime': '运行时设置',
  'field.id': 'ID',
  'field.name': '名称',
  'field.source': '来源',
  'field.address': '地址',
  'field.status': '状态',
  'field.updated': '更新时间',
  'field.type': '类型',
  'field.created': '创建时间',
  'field.root': '根目录',
  'field.schedule': '计划',
  'field.timezone': '时区',
  'field.maxRuntime': '最长运行',
  'field.maxRuntimeSeconds': '最长运行(秒)',
  'field.retries': '重试次数',
  'field.enabled': '启用',
  'field.action': '操作',
  'field.credential': '凭据',
  'field.job': '任务',
  'field.progress': '进度',
  'field.started': '开始时间',
  'field.finished': '结束时间',
  'field.error': '错误',
  'field.manifest': '清单',
  'field.files': '文件数',
  'field.size': '大小',
  'field.modified': '修改时间',
  'field.snapshot': '快照',
  'field.path': '路径',
  'field.target': '目标',
  'field.task': '任务',
  'field.mode': '模式',
  'field.segmentSize': '分段大小',
  'field.segments': '分段',
  'field.logical': '逻辑大小',
  'field.compressed': '压缩后',
  'field.manifests': '清单数',
  'field.retention': '保留',
  'field.expired': '已过期',
  'field.utilization': '利用率',
  'field.orphans': '孤儿分块',
  'field.verified': '已校验',
  'field.verifyErrors': '校验错误',
  'field.compacted': '已整理',
  'field.compactSkipped': '整理跳过',
  'field.segment': '分段',
  'field.chunk': '分块',
  'field.compression': '压缩',
  'field.adminUsername': '管理员用户名',
  'field.sessionTTL': '会话 TTL(小时)',
  'field.currentPassword': '当前密码',
  'field.newPassword': '新密码',
  'field.keepLast': '保留最近',
  'field.daily': '每日保留',
  'field.weekly': '每周保留',
  'field.baseURL': '基础 URL',
  'field.bearerToken': 'Bearer Token',
  'field.clientID': 'Client ID',
  'field.clientSecret': 'Client Secret',
  'field.subject': '主体',
  'field.privateKey': '私钥',
  'field.explicitTLS': '显式 TLS',
  'field.skipTLSVerify': '跳过 TLS 校验',
  'field.sqlite': 'SQLite',
  'source.local': '本地',
  'source.sftp': 'SFTP',
  'source.ftp': 'FTP',
  'source.ftps': 'FTPS',
  'source.webdav': 'WebDAV',
  'source.agent': '客户端',
  'entry.file': '文件',
  'entry.dir': '目录',
  'phase.scanning': '扫描中',
  'phase.manifest': '生成清单',
  'phase.uploading': '上传中',
  'phase.running': '运行中',
  'phase.completed': '已完成',
  'phase.failed': '失败',
  'maintenance.retention': '保留策略',
  'maintenance.verify': '校验',
  'maintenance.compact': '压缩整理',
  'placeholder.hostAddress': 'host:port 或标签',
  'placeholder.hostLabel': '主机标签',
  'message.credentialCreated': '凭据已创建',
  'message.agentSecretStored': '这个客户端 Secret 已保存，可在已保存凭据中再次查看。',
  'message.agentClientCreated': '客户端已创建，凭据已生成',
  'message.hostCreated': '主机已创建',
  'message.jobCreated': '任务已创建',
  'message.jobUpdated': '任务 #{id} 已更新',
  'message.jobToggled': '任务 #{id} 已{status}',
  'message.jobEnabled': '启用',
  'message.jobDisabled': '禁用',
  'message.verifiedChunks': '已校验 {count} 个分块',
  'message.compactedChunks': '已整理 {count} 个分块',
  'message.maintenanceExpired': '维护已过期 {count} 个快照',
  'message.activeRunsExist': '存在运行中的任务',
  'message.settingsSaved': '设置已保存',
  'message.restoreStatus': '恢复 #{id} {status}',
  'message.runStatus': '运行 #{id} {status}',
  'unit.files': '个文件',
  'unit.chunks': '个分块',
  'summary.retention': '{active} 活跃 / {deleted} 已删除',
  'summary.compacted': '{chunks} 个分块 / {bytes}',
  'status.ok': '正常',
  'status.loading': '加载中',
  'status.enabled': '已启用',
  'status.disabled': '已禁用',
  'status.pending': '等待中',
  'status.running': '运行中',
  'status.completed': '已完成',
  'status.failed': '失败',
  'status.error': '错误',
  'status.accepted': '已接受',
  'status.unknown': '未知',
  'status.online': '在线',
  'status.canceled': '已取消'
};

const messages = { en, zh } as const;
type Locale = keyof typeof messages;
type MessageKey = keyof typeof en;

const localeStorageKey = 'turbk.locale';

function initialLocale(): Locale {
  if (typeof window !== 'undefined') {
    const saved = window.localStorage.getItem(localeStorageKey);
    if (saved === 'zh' || saved === 'en') return saved;
    if (window.navigator.language.toLowerCase().startsWith('zh')) return 'zh';
  }
  return 'en';
}

const locale = ref<Locale>(initialLocale());

function t(key: MessageKey, params: Record<string, string | number> = {}) {
  const template = messages[locale.value][key] ?? messages.en[key] ?? key;
  return template.replace(/\{(\w+)\}/g, (match, name: string) => String(params[name] ?? match));
}

function setLocale(nextLocale: Locale) {
  locale.value = nextLocale;
}

watch(
  locale,
  (nextLocale) => {
    if (typeof document !== 'undefined') document.documentElement.lang = nextLocale === 'zh' ? 'zh-CN' : 'en';
    if (typeof window !== 'undefined') window.localStorage.setItem(localeStorageKey, nextLocale);
  },
  { immediate: true }
);

const pages = [
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

type PageKey = (typeof pages)[number]['key'];

const activePage = ref<PageKey>('dashboard');
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
const showHostCreate = ref(false);
const hostActionMessage = ref('');
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
const agentCredentialClientId = ref('');
const agentCredentialSecret = ref('');
const jobName = ref('');
const jobSourceType = ref('local');
const jobRoot = ref('');
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

const statRows = computed(() => [
  { label: t('nav.hosts'), value: counts.value.hosts, detail: t('dashboard.hostsDetail'), icon: Server },
  { label: t('nav.jobs'), value: counts.value.jobs, detail: t('dashboard.jobsDetail'), icon: CalendarClock },
  { label: t('nav.runs'), value: counts.value.runs, detail: t('dashboard.runsDetail'), icon: History },
  { label: t('nav.snapshots'), value: counts.value.snapshots, detail: t('dashboard.snapshotsDetail'), icon: Archive }
]);

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
    health.value = nextHealth;
    bootstrap.value = nextBootstrap;
    hydrateSettingsForm(nextBootstrap);
    storageHealth.value = nextStorage;
    hosts.value = hostResp.hosts;
    credentials.value = credentialResp.credentials;
    jobs.value = jobResp.jobs;
    runs.value = runResp.runs;
    snapshots.value = snapshotResp.snapshots;
    restoreTasks.value = restoreTaskResp.tasks;
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
    activePage.value = 'dashboard';
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
  activePage.value = 'restore';
}

async function createCredential() {
  error.value = '';
  actionMessage.value = '';
  agentCredentialClientId.value = '';
  agentCredentialSecret.value = '';
  try {
    const payload: Record<string, unknown> = {};
    if (credentialType.value === 'agent') {
      payload.subject = credentialSubject.value.trim();
    } else if (credentialType.value === 'webdav') {
      payload.base_url = credentialAddress.value.trim();
      payload.username = credentialUsername.value.trim();
      payload.password = credentialPassword.value;
      payload.bearer_token = credentialBearerToken.value.trim();
    } else {
      payload.address = credentialAddress.value.trim();
      payload.username = credentialUsername.value.trim();
      payload.password = credentialPassword.value;
      payload.private_key = credentialPrivateKey.value;
      if (credentialType.value === 'ftps') {
        payload.tls = true;
        payload.explicit_tls = credentialExplicitTLS.value;
        payload.skip_tls_verify = credentialSkipTLSVerify.value;
      }
    }
    const result = await api.createCredential({
      name: credentialName.value.trim(),
      type: credentialType.value,
      payload
    });
    if (credentialType.value === 'agent') {
      agentCredentialClientId.value = result.credential.client_id ?? '';
      agentCredentialSecret.value = result.client_secret ?? '';
    }
    actionMessage.value = t('message.credentialCreated');
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
	try {
		const name = hostName.value.trim();
		const sourceType = hostSourceType.value;
		const payload: { name: string; source_type: string; address?: string } = {
			name,
			source_type: sourceType
		};
		if (sourceType !== 'agent') {
			payload.address = hostAddress.value.trim();
		}
		await api.createHost(payload);
		if (sourceType === 'agent') {
			const result = await api.createCredential({
				name,
        type: 'agent',
        payload: { subject: name }
      });
      agentCredentialClientId.value = result.credential.client_id ?? '';
      agentCredentialSecret.value = result.client_secret ?? '';
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
      source_type: string;
      source_config: Record<string, unknown>;
      credential_id?: number;
      enabled: boolean;
      schedule?: string;
      timezone?: string;
      max_runtime_seconds: number;
      retry_attempts: number;
    } = {
      name: jobName.value.trim(),
      source_type: jobSourceType.value,
      source_config: { root: jobRoot.value.trim() },
      enabled: true,
      max_runtime_seconds: nonNegativeInt(jobMaxRuntimeSeconds.value),
      retry_attempts: nonNegativeInt(jobRetryAttempts.value)
    };
    if (jobSourceType.value !== 'local') {
      payload.credential_id = Number(jobCredentialId.value);
    }
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

const TablePanel = defineComponent({
  props: {
    title: { type: String, required: true },
    empty: { type: Boolean, required: true }
  },
  setup(props, { slots }) {
    return () =>
      h('section', { class: 'panel' }, [
        h('div', { class: 'panel-title' }, [h('h2', props.title), slots.actions?.()]),
        props.empty
          ? h('div', { class: 'empty-state' }, [h('span', t('common.noRecords'))])
          : h('div', { class: 'table-wrap' }, [h('table', slots.default?.())])
      ]);
  }
});

onMounted(checkSession);
</script>

<template>
  <div v-if="checkingSession" class="login-shell">
    <section class="login-panel">
      <div class="login-topline">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div class="language-switch" role="group" :aria-label="t('language')">
          <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
          <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
        </div>
      </div>
      <h1>Turbk</h1>
      <div class="status-pill">
        <Activity :size="16" />
        <span>{{ t('common.loading') }}</span>
      </div>
    </section>
  </div>

  <div v-else-if="!authenticated" class="login-shell">
    <form class="login-panel" @submit.prevent="login">
      <div class="login-topline">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div class="language-switch" role="group" :aria-label="t('language')">
          <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
          <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
        </div>
      </div>
      <h1>Turbk</h1>
      <label class="field">
        <span>{{ t('auth.username') }}</span>
        <input v-model="loginUsername" type="text" autocomplete="username" required />
      </label>
      <label class="field">
        <span>{{ t('auth.password') }}</span>
        <input v-model="loginPassword" type="password" autocomplete="current-password" required />
      </label>
      <div v-if="loginError" class="error-bar">
        {{ loginError }}
      </div>
      <button class="text-button primary login-button" type="submit" :disabled="loginLoading">
        <User :size="16" />
        <span>{{ loginLoading ? t('auth.signingIn') : t('auth.signIn') }}</span>
      </button>
    </form>
  </div>

  <div v-else class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div>
          <strong>Turbk</strong>
          <span>{{ t('brand.subtitle') }}</span>
        </div>
      </div>

      <nav class="nav-list" :aria-label="t('nav.primary')">
        <button
          v-for="page in pages"
          :key="page.key"
          class="nav-item"
          :class="{ active: activePage === page.key }"
          type="button"
          @click="activePage = page.key"
        >
          <component :is="page.icon" :size="18" />
          <span>{{ t(page.labelKey) }}</span>
        </button>
      </nav>
    </aside>

    <main class="workspace">
      <header class="topbar">
        <div>
          <p class="eyebrow">{{ activeTitle }}</p>
          <h1>{{ activeTitle }}</h1>
        </div>
        <div class="top-actions">
          <div class="language-switch" role="group" :aria-label="t('language')">
            <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
            <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
          </div>
          <div class="user-chip">
            <User :size="16" />
            <span>{{ currentUser }}</span>
          </div>
          <div class="status-pill" :class="health?.status === 'ok' ? 'ok' : 'warn'">
            <CheckCircle2 v-if="health?.status === 'ok'" :size="16" />
            <Activity v-else :size="16" />
            <span>{{ statusText(health?.status ?? 'loading') }}</span>
          </div>
          <button class="icon-button" type="button" :title="t('common.refresh')" :disabled="loading" @click="refresh">
            <RefreshCw :class="{ spin: loading }" :size="18" />
          </button>
          <button class="icon-button" type="button" :title="t('common.logout')" @click="logout">
            <LogOut :size="18" />
          </button>
        </div>
      </header>

      <div v-if="error" class="error-bar">
        {{ error }}
      </div>

      <section v-if="activePage === 'dashboard'" class="view">
        <div class="status-grid">
          <article v-for="row in statRows" :key="row.label" class="stat-card">
            <component :is="row.icon" :size="20" />
            <div>
              <span>{{ row.label }}</span>
              <strong>{{ row.value }}</strong>
              <small>{{ row.detail }}</small>
            </div>
          </article>
        </div>

        <div class="two-column">
          <section class="panel">
            <div class="panel-title">
              <h2>{{ t('dashboard.system') }}</h2>
              <span>{{ health?.version ?? '-' }}</span>
            </div>
            <dl class="details">
              <div>
                <dt>{{ t('dashboard.database') }}</dt>
                <dd>{{ health?.database ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('dashboard.started') }}</dt>
                <dd>{{ formatTime(health?.started_at) }}</dd>
              </div>
              <div>
                <dt>{{ t('dashboard.repository') }}</dt>
                <dd>{{ bootstrap?.paths.repo_dir ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('dashboard.state') }}</dt>
                <dd>{{ bootstrap?.paths.state_dir ?? '-' }}</dd>
              </div>
            </dl>
          </section>

          <section class="panel">
            <div class="panel-title">
              <h2>{{ t('dashboard.backupModes') }}</h2>
              <span>Phase 3</span>
            </div>
            <div class="mode-grid">
              <div class="mode-row">
                <Server :size="18" />
                <span>SFTP</span>
              </div>
              <div class="mode-row">
                <UploadCloud :size="18" />
                <span>FTP/FTPS</span>
              </div>
              <div class="mode-row">
                <Database :size="18" />
                <span>WebDAV</span>
              </div>
              <div class="mode-row">
                <Activity :size="18" />
                <span>{{ t('dashboard.agentHTTP') }}</span>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section v-else-if="activePage === 'hosts'" class="view">
        <TablePanel :title="t('nav.hosts')" :empty="hosts.length === 0">
          <template #actions>
            <div class="panel-actions">
              <span v-if="hostActionMessage" class="action-note">{{ hostActionMessage }}</span>
              <button class="text-button primary" type="button" @click="showHostCreate = !showHostCreate">
                <X v-if="showHostCreate" :size="16" />
                <Plus v-else :size="16" />
                <span>{{ showHostCreate ? t('common.close') : t('hosts.new') }}</span>
              </button>
            </div>
          </template>
          <thead>
            <tr>
              <th>{{ t('field.name') }}</th>
              <th>{{ t('field.source') }}</th>
              <th>{{ t('field.address') }}</th>
              <th>{{ t('field.status') }}</th>
              <th>{{ t('field.updated') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="host in hosts" :key="host.id">
              <td>{{ host.name }}</td>
              <td>{{ sourceTypeLabel(host.source_type) }}</td>
              <td>{{ nullText(host.address) }}</td>
              <td><span class="tag">{{ statusText(host.status) }}</span></td>
              <td>{{ formatTime(host.updated_at) }}</td>
            </tr>
          </tbody>
        </TablePanel>
        <section v-if="showHostCreate" class="panel">
          <div class="panel-title">
            <h2>{{ t('hosts.create') }}</h2>
          </div>
          <form class="inline-form" @submit.prevent="createHost">
            <label class="field">
              <span>{{ t('field.name') }}</span>
              <input v-model="hostName" type="text" required />
            </label>
            <label class="field">
              <span>{{ t('field.source') }}</span>
              <select v-model="hostSourceType">
                <option value="local">{{ sourceTypeLabel('local') }}</option>
                <option value="sftp">SFTP</option>
                <option value="ftp">FTP</option>
                <option value="ftps">FTPS</option>
                <option value="webdav">WebDAV</option>
                <option value="agent">{{ sourceTypeLabel('agent') }}</option>
              </select>
            </label>
	            <label v-if="hostSourceType !== 'agent'" class="field wide">
	              <span>{{ t('field.address') }}</span>
	              <input v-model="hostAddress" type="text" :placeholder="t('placeholder.hostAddress')" />
	            </label>
            <button class="text-button primary" type="submit">
              <Server :size="16" />
              <span>{{ t('common.create') }}</span>
            </button>
          </form>
        </section>
        <section v-if="agentCredentialClientId && agentCredentialSecret" class="panel">
          <div class="panel-title">
            <h2>{{ t('hosts.clientCredentials') }}</h2>
            <span>{{ t('message.agentSecretStored') }}</span>
          </div>
          <dl class="details">
            <div>
              <dt>{{ t('field.clientID') }}</dt>
              <dd>{{ agentCredentialClientId }}</dd>
            </div>
            <div>
              <dt>{{ t('field.clientSecret') }}</dt>
              <dd>{{ agentCredentialSecret }}</dd>
            </div>
          </dl>
        </section>
      </section>

      <section v-else-if="activePage === 'credentials'" class="view">
        <section class="panel">
          <div class="panel-title">
            <h2>{{ t('nav.credentials') }}</h2>
            <span>{{ counts.credentials }}</span>
          </div>
          <form class="inline-form credential-form" @submit.prevent="createCredential">
            <label class="field">
              <span>{{ t('field.name') }}</span>
              <input v-model="credentialName" type="text" required />
            </label>
            <label class="field">
              <span>{{ t('field.type') }}</span>
              <select v-model="credentialType">
                <option value="sftp">SFTP</option>
                <option value="ftp">FTP</option>
                <option value="ftps">FTPS</option>
                <option value="webdav">WebDAV</option>
                <option value="agent">{{ sourceTypeLabel('agent') }}</option>
              </select>
            </label>
            <label v-if="credentialType !== 'agent'" class="field wide">
              <span>{{ credentialType === 'webdav' ? t('field.baseURL') : t('field.address') }}</span>
              <input v-model="credentialAddress" type="text" required />
            </label>
            <label v-if="credentialType !== 'agent'" class="field">
              <span>{{ t('auth.username') }}</span>
              <input v-model="credentialUsername" type="text" />
            </label>
            <label v-if="credentialType !== 'agent'" class="field">
              <span>{{ t('auth.password') }}</span>
              <input v-model="credentialPassword" type="password" />
            </label>
            <label v-if="credentialType === 'agent'" class="field">
              <span>{{ t('field.subject') }}</span>
              <input v-model="credentialSubject" type="text" :placeholder="t('placeholder.hostLabel')" />
            </label>
            <label v-if="credentialType === 'sftp'" class="field wide">
              <span>{{ t('field.privateKey') }}</span>
              <textarea v-model="credentialPrivateKey" rows="3"></textarea>
            </label>
            <label v-if="credentialType === 'webdav'" class="field">
              <span>{{ t('field.bearerToken') }}</span>
              <input v-model="credentialBearerToken" type="password" />
            </label>
            <label v-if="credentialType === 'ftps'" class="field checkbox-field">
              <input v-model="credentialExplicitTLS" type="checkbox" />
              <span>{{ t('field.explicitTLS') }}</span>
            </label>
            <label v-if="credentialType === 'ftps'" class="field checkbox-field">
              <input v-model="credentialSkipTLSVerify" type="checkbox" />
              <span>{{ t('field.skipTLSVerify') }}</span>
            </label>
            <button class="text-button primary" type="submit">
              <KeyRound :size="16" />
              <span>{{ t('common.create') }}</span>
            </button>
          </form>
          <dl v-if="agentCredentialClientId && agentCredentialSecret" class="details result-block">
            <div>
              <dt>{{ t('field.clientID') }}</dt>
              <dd>{{ agentCredentialClientId }}</dd>
            </div>
            <div>
              <dt>{{ t('field.clientSecret') }}</dt>
              <dd>{{ agentCredentialSecret }}</dd>
            </div>
            <div>
              <dt>{{ t('field.status') }}</dt>
              <dd>{{ t('message.agentSecretStored') }}</dd>
            </div>
          </dl>
        </section>
        <TablePanel :title="t('credentials.saved')" :empty="credentials.length === 0">
          <thead>
            <tr>
              <th>{{ t('field.name') }}</th>
              <th>{{ t('field.type') }}</th>
              <th>{{ t('field.clientID') }}</th>
              <th>{{ t('field.clientSecret') }}</th>
              <th>{{ t('field.created') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="credential in credentials" :key="credential.id">
              <td>{{ credential.name }}</td>
              <td>{{ sourceTypeLabel(credential.type) }}</td>
              <td>{{ credential.client_id ?? '-' }}</td>
              <td>{{ credential.client_secret ?? '-' }}</td>
              <td>{{ formatTime(credential.created_at) }}</td>
            </tr>
          </tbody>
        </TablePanel>
      </section>

      <section v-else-if="activePage === 'jobs'" class="view">
        <TablePanel :title="t('nav.jobs')" :empty="jobs.length === 0">
          <template #actions>
            <div class="panel-actions">
              <span v-if="jobActionMessage" class="action-note">{{ jobActionMessage }}</span>
              <button class="text-button primary" type="button" @click="showJobCreate = !showJobCreate">
                <X v-if="showJobCreate" :size="16" />
                <Plus v-else :size="16" />
                <span>{{ showJobCreate ? t('common.close') : t('jobs.new') }}</span>
              </button>
            </div>
          </template>
          <thead>
            <tr>
              <th>{{ t('field.name') }}</th>
              <th>{{ t('field.source') }}</th>
              <th>{{ t('field.root') }}</th>
              <th>{{ t('field.schedule') }}</th>
              <th>{{ t('field.timezone') }}</th>
              <th>{{ t('field.maxRuntime') }}</th>
              <th>{{ t('field.retries') }}</th>
              <th>{{ t('field.enabled') }}</th>
              <th>{{ t('field.updated') }}</th>
              <th>{{ t('field.action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="job in jobs" :key="job.id">
              <td v-if="editJobId === job.id">
                <input v-model="editJobName" class="table-input" type="text" required />
              </td>
              <td v-else>{{ job.name }}</td>
              <td>{{ sourceTypeLabel(job.source_type) }}</td>
              <td v-if="editJobId === job.id">
                <input v-model="editJobRoot" class="table-input" type="text" required />
              </td>
              <td v-else>{{ sourceRoot(job) }}</td>
              <td v-if="editJobId === job.id">
                <input v-model="editJobSchedule" class="table-input" type="text" placeholder="0 2 * * *" />
              </td>
              <td v-else>{{ nullText(job.schedule) }}</td>
              <td v-if="editJobId === job.id">
                <input v-model="editJobTimezone" class="table-input" type="text" placeholder="Asia/Shanghai" />
              </td>
              <td v-else>{{ job.timezone }}</td>
              <td v-if="editJobId === job.id">
                <input v-model.number="editJobMaxRuntimeSeconds" class="table-input" type="number" min="0" step="1" />
              </td>
              <td v-else>{{ job.max_runtime_seconds > 0 ? `${job.max_runtime_seconds}s` : '-' }}</td>
              <td v-if="editJobId === job.id">
                <input v-model.number="editJobRetryAttempts" class="table-input" type="number" min="0" step="1" />
              </td>
              <td v-else>{{ job.retry_attempts > 0 ? job.retry_attempts : '-' }}</td>
              <td v-if="editJobId === job.id">
                <label class="table-check">
                  <input v-model="editJobEnabled" type="checkbox" />
                  <span>{{ yesNo(editJobEnabled) }}</span>
                </label>
              </td>
              <td v-else><span class="tag">{{ yesNo(job.enabled) }}</span></td>
              <td>{{ formatTime(job.updated_at) }}</td>
              <td>
                <div v-if="editJobId === job.id" class="button-row">
                  <button class="text-button primary" type="button" :disabled="savingJobId === job.id" @click="saveJob(job)">
                    <Save :size="16" />
                    <span>{{ savingJobId === job.id ? t('common.saving') : t('common.save') }}</span>
                  </button>
                  <button class="text-button" type="button" :disabled="savingJobId === job.id" @click="cancelEditJob">
                    <X :size="16" />
                    <span>{{ t('common.cancel') }}</span>
                  </button>
                </div>
                <div v-else class="button-row">
                  <button class="text-button" type="button" :disabled="runningJobId === job.id || job.source_type === 'agent'" @click="runJob(job)">
                    <Play :size="16" />
                    <span>{{ job.source_type === 'agent' ? t('common.agent') : runningJobId === job.id ? t('common.running') : t('common.run') }}</span>
                  </button>
                  <button class="text-button" type="button" :disabled="savingJobId === job.id" @click="toggleJob(job)">
                    <Power :size="16" />
                    <span>{{ job.enabled ? t('common.disable') : t('common.enable') }}</span>
                  </button>
                  <button v-if="job.source_type !== 'agent'" class="text-button" type="button" :disabled="savingJobId === job.id" @click="startEditJob(job)">
                    <Pencil :size="16" />
                    <span>{{ t('common.edit') }}</span>
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </TablePanel>
        <section v-if="showJobCreate" class="panel">
          <div class="panel-title">
            <h2>{{ t('jobs.create') }}</h2>
          </div>
          <form class="inline-form" @submit.prevent="createJob">
            <label class="field">
              <span>{{ t('field.name') }}</span>
              <input v-model="jobName" type="text" required />
            </label>
            <label class="field">
              <span>{{ t('field.source') }}</span>
              <select v-model="jobSourceType">
                <option value="local">{{ sourceTypeLabel('local') }}</option>
                <option value="sftp">SFTP</option>
                <option value="ftp">FTP</option>
                <option value="ftps">FTPS</option>
                <option value="webdav">WebDAV</option>
              </select>
            </label>
            <label v-if="jobSourceType !== 'local'" class="field">
              <span>{{ t('field.credential') }}</span>
              <select v-model="jobCredentialId" required>
                <option value="">{{ t('common.select') }}</option>
                <option v-for="credential in credentials.filter((item) => item.type === jobSourceType)" :key="credential.id" :value="credential.id">
                  {{ credential.name }}
                </option>
              </select>
            </label>
            <label class="field wide">
              <span>{{ t('field.root') }}</span>
              <input v-model="jobRoot" type="text" required placeholder="/srv/data" />
            </label>
            <label class="field">
              <span>{{ t('field.schedule') }}</span>
              <input v-model="jobSchedule" type="text" placeholder="0 2 * * *" />
            </label>
            <label class="field">
              <span>{{ t('field.timezone') }}</span>
              <input v-model="jobTimezone" type="text" placeholder="Asia/Shanghai" />
            </label>
            <label class="field">
              <span>{{ t('field.maxRuntimeSeconds') }}</span>
              <input v-model.number="jobMaxRuntimeSeconds" type="number" min="0" step="1" />
            </label>
            <label class="field">
              <span>{{ t('field.retries') }}</span>
              <input v-model.number="jobRetryAttempts" type="number" min="0" step="1" />
            </label>
            <button class="text-button primary" type="submit">
              <CalendarClock :size="16" />
              <span>{{ t('common.create') }}</span>
            </button>
          </form>
        </section>
      </section>

      <section v-else-if="activePage === 'runs'" class="view">
        <TablePanel :title="t('nav.runs')" :empty="runs.length === 0">
          <thead>
            <tr>
              <th>{{ t('field.id') }}</th>
              <th>{{ t('field.job') }}</th>
              <th>{{ t('field.status') }}</th>
              <th>{{ t('field.progress') }}</th>
              <th>{{ t('field.started') }}</th>
              <th>{{ t('field.finished') }}</th>
              <th>{{ t('field.error') }}</th>
              <th>{{ t('field.action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="run in runs" :key="run.id">
              <td>#{{ run.id }}</td>
              <td>{{ nullText(run.job_id) }}</td>
              <td><span class="tag">{{ statusText(run.status) }}</span></td>
              <td>{{ runProgressText(run) }}</td>
              <td>{{ nullText(run.started_at) }}</td>
              <td>{{ nullText(run.finished_at) }}</td>
              <td>{{ nullText(run.error_message) }}</td>
              <td>
                <button class="text-button" type="button" :disabled="loadingRunLogs && selectedRunId === run.id" @click="viewRunLogs(run)">
                  <ScrollText :size="16" />
                  <span>{{ loadingRunLogs && selectedRunId === run.id ? t('common.loading') : t('common.logs') }}</span>
                </button>
              </td>
            </tr>
          </tbody>
        </TablePanel>
        <section v-if="selectedRunId" class="panel">
          <div class="panel-title">
            <h2>{{ t('runs.logsTitle', { id: selectedRunId }) }}</h2>
            <span>{{ runLogs.length }}</span>
          </div>
          <div v-if="runLogs.length === 0" class="empty-state">
            <span>{{ t('common.noRecords') }}</span>
          </div>
          <ul v-else class="log-list">
            <li v-for="log in runLogs" :key="log.id">
              <span class="tag">{{ log.level }}</span>
              <time>{{ formatTime(log.created_at) }}</time>
              <p>{{ log.message }}</p>
            </li>
          </ul>
        </section>
      </section>

      <section v-else-if="activePage === 'snapshots'" class="view">
        <TablePanel :title="t('nav.snapshots')" :empty="snapshots.length === 0">
          <thead>
            <tr>
              <th>{{ t('field.id') }}</th>
              <th>{{ t('field.job') }}</th>
              <th>{{ t('field.manifest') }}</th>
              <th>{{ t('field.source') }}</th>
              <th>{{ t('field.files') }}</th>
              <th>{{ t('field.size') }}</th>
              <th>{{ t('field.created') }}</th>
              <th>{{ t('field.action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="snapshot in snapshots" :key="snapshot.id">
              <td>#{{ snapshot.id }}</td>
              <td>{{ nullText(snapshot.job_id) }}</td>
              <td>{{ snapshot.manifest_ref }}</td>
              <td>{{ sourceTypeLabel(snapshot.source_type) }}</td>
              <td>{{ snapshot.file_count }}</td>
              <td>{{ formatBytes(snapshot.total_size) }}</td>
              <td>{{ formatTime(snapshot.created_at) }}</td>
              <td>
                <div class="button-row">
                  <button class="text-button" type="button" @click="browseSnapshot(snapshot, '.')">
                    <FolderOpen :size="16" />
                    <span>{{ t('common.browse') }}</span>
                  </button>
                  <a class="text-button" :href="snapshotDownloadURL(snapshot, '.')" download>
                    <Download :size="16" />
                    <span>{{ t('common.download') }}</span>
                  </a>
                  <button class="text-button" type="button" @click="selectRestore(snapshot, '.')">
                    <RotateCcw :size="16" />
                    <span>{{ t('common.restore') }}</span>
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </TablePanel>
        <section v-if="selectedSnapshot" class="panel">
          <div class="panel-title">
            <h2>{{ t('snapshots.title', { id: selectedSnapshot.id }) }}</h2>
            <div class="button-row">
              <button class="text-button" type="button" :disabled="snapshotTreePath === '.' || loadingTree" @click="browseSnapshot(selectedSnapshot, parentTreePath(snapshotTreePath))">
                <ArrowUp :size="16" />
                <span>{{ t('common.up') }}</span>
              </button>
              <a class="text-button" :href="snapshotDownloadURL(selectedSnapshot, snapshotTreePath)" download>
                <Download :size="16" />
                <span>{{ t('common.download') }}</span>
              </a>
              <button class="text-button" type="button" @click="selectRestore(selectedSnapshot, snapshotTreePath)">
                <RotateCcw :size="16" />
                <span>{{ t('common.restore') }}</span>
              </button>
            </div>
          </div>
          <div class="path-chip">{{ snapshotTreePath }}</div>
          <div v-if="snapshotTreeEntries.length === 0" class="empty-state">
            <span>{{ t('common.noRecords') }}</span>
          </div>
          <div v-else class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{{ t('field.name') }}</th>
                  <th>{{ t('field.type') }}</th>
                  <th>{{ t('field.size') }}</th>
                  <th>{{ t('field.modified') }}</th>
                  <th>{{ t('field.action') }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="entry in snapshotTreeEntries" :key="entry.path">
                  <td>{{ entry.name }}</td>
                  <td><span class="tag">{{ entryTypeLabel(entry.type) }}</span></td>
                  <td>{{ entry.type === 'file' ? formatBytes(entry.size) : '-' }}</td>
                  <td>{{ formatTime(entry.mod_time) }}</td>
                  <td>
                    <div class="button-row">
                      <button v-if="entry.type === 'dir'" class="text-button" type="button" :disabled="loadingTree" @click="browseSnapshot(selectedSnapshot, entry.path)">
                        <FolderOpen :size="16" />
                        <span>{{ t('common.open') }}</span>
                      </button>
                      <a class="text-button" :href="snapshotDownloadURL(selectedSnapshot, entry.path)" download>
                        <Download :size="16" />
                        <span>{{ t('common.download') }}</span>
                      </a>
                      <button class="text-button" type="button" @click="selectRestore(selectedSnapshot, entry.path)">
                        <RotateCcw :size="16" />
                        <span>{{ t('common.restore') }}</span>
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </section>

      <section v-else-if="activePage === 'restore'" class="view">
        <section class="panel">
          <div class="panel-title">
            <h2>{{ t('nav.restore') }}</h2>
            <span v-if="actionMessage">{{ actionMessage }}</span>
          </div>
          <form class="inline-form" @submit.prevent="restorePath">
            <label class="field">
              <span>{{ t('field.snapshot') }}</span>
              <select v-model="restoreSnapshotId" required>
                <option value="">{{ t('common.select') }}</option>
                <option v-for="snapshot in snapshots" :key="snapshot.id" :value="snapshot.id">
                  #{{ snapshot.id }} {{ snapshot.manifest_ref }}
                </option>
              </select>
            </label>
            <label class="field">
              <span>{{ t('field.path') }}</span>
              <input v-model="restoreEntryPath" type="text" required />
            </label>
            <label class="field wide">
              <span>{{ t('field.target') }}</span>
              <input v-model="restoreTargetPath" type="text" required />
            </label>
            <button class="text-button primary" type="submit" :disabled="restoring">
              <RotateCcw :size="16" />
              <span>{{ restoring ? t('common.running') : t('common.restore') }}</span>
            </button>
          </form>
          <dl v-if="restoreResult" class="details result-block">
            <div>
              <dt>{{ t('field.task') }}</dt>
              <dd>#{{ restoreResult.task.id }} {{ statusText(restoreResult.task.status) }}</dd>
            </div>
            <div>
              <dt>{{ t('field.target') }}</dt>
              <dd>{{ restoreResult.task.target_path }}</dd>
            </div>
          </dl>
        </section>
        <section class="panel">
          <div class="panel-title">
            <h2>{{ t('restore.roots') }}</h2>
            <span>{{ bootstrap?.paths.restore_roots.length ?? 0 }}</span>
          </div>
          <ul class="path-list">
            <li v-for="root in bootstrap?.paths.restore_roots ?? []" :key="root">{{ root }}</li>
          </ul>
        </section>
        <TablePanel :title="t('restore.tasks')" :empty="restoreTasks.length === 0">
          <thead>
            <tr>
              <th>{{ t('field.id') }}</th>
              <th>{{ t('field.snapshot') }}</th>
              <th>{{ t('field.status') }}</th>
              <th>{{ t('field.target') }}</th>
              <th>{{ t('field.updated') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="task in restoreTasks" :key="task.id">
              <td>#{{ task.id }}</td>
              <td>#{{ task.snapshot_id }}</td>
              <td><span class="tag">{{ statusText(task.status) }}</span></td>
              <td>{{ task.target_path }}</td>
              <td>{{ formatTime(task.updated_at) }}</td>
            </tr>
          </tbody>
        </TablePanel>
      </section>

      <section v-else-if="activePage === 'storage'" class="view">
        <div class="two-column">
          <section class="panel">
            <div class="panel-title">
              <h2>{{ t('dashboard.repository') }}</h2>
              <span>{{ storageHealth?.segment.writeMode ?? '-' }}</span>
            </div>
            <dl class="details">
              <div>
                <dt>{{ t('field.path') }}</dt>
                <dd>{{ storageHealth?.repo.path ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('field.segmentSize') }}</dt>
                <dd>{{ storageHealth?.segment.size ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('field.segments') }}</dt>
                <dd>{{ storageHealth?.segment?.count ?? 0 }} / {{ formatBytes(storageHealth?.segment?.bytes ?? 0) }}</dd>
              </div>
              <div>
                <dt>{{ t('field.mode') }}</dt>
                <dd>{{ storageHealth?.repo.mode ?? '-' }}</dd>
              </div>
            </dl>
          </section>
          <section class="panel">
            <div class="panel-title">
              <h2>{{ t('storage.index') }}</h2>
              <span>{{ storageHealth?.chunks?.count ?? 0 }} {{ t('unit.chunks') }}</span>
            </div>
            <dl class="details">
              <div>
                <dt>{{ t('field.logical') }}</dt>
                <dd>{{ formatBytes(storageHealth?.chunks?.logical_bytes ?? 0) }}</dd>
              </div>
              <div>
                <dt>{{ t('field.compressed') }}</dt>
                <dd>{{ formatBytes(storageHealth?.chunks?.compressed_bytes ?? 0) }}</dd>
              </div>
              <div>
                <dt>{{ t('field.manifests') }}</dt>
                <dd>{{ storageHealth?.manifests?.count ?? 0 }}</dd>
              </div>
              <div>
                <dt>{{ t('field.sqlite') }}</dt>
                <dd>{{ formatBytes(storageHealth?.sqlite.size ?? 0) }}</dd>
              </div>
            </dl>
          </section>
	        </div>
	        <section class="panel">
	          <div class="panel-title">
	            <h2>{{ t('storage.maintenance') }}</h2>
	            <div class="button-row">
	              <button class="text-button" type="button" :disabled="maintenanceRunning" @click="runMaintenance('retention')">
	                <Wrench :size="16" />
	                <span>{{ maintenanceRunning ? t('common.running') : t('common.run') }}</span>
	              </button>
	              <button class="text-button" type="button" :disabled="maintenanceRunning" @click="runMaintenance('verify')">
	                <Shield :size="16" />
	                <span>{{ t('common.verify') }}</span>
	              </button>
	              <button class="text-button" type="button" :disabled="maintenanceRunning" @click="runMaintenance('compact')">
	                <Database :size="16" />
	                <span>{{ t('common.compact') }}</span>
	              </button>
	            </div>
	          </div>
	          <dl class="details">
	            <div>
	              <dt>{{ t('field.mode') }}</dt>
	              <dd>{{ maintenanceModeText(maintenanceReport?.mode) }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.retention') }}</dt>
	              <dd>
	                {{ maintenanceReport ? t('summary.retention', { active: maintenanceReport.retention.active_snapshots, deleted: maintenanceReport.retention.deleted_snapshots }) : '-' }}
	              </dd>
	            </div>
	            <div>
	              <dt>{{ t('field.expired') }}</dt>
	              <dd>{{ maintenanceReport?.retention.expired_snapshots ?? '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.utilization') }}</dt>
	              <dd>{{ maintenanceReport ? formatPercent(maintenanceReport.segment.utilization) : '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.orphans') }}</dt>
	              <dd>{{ maintenanceReport?.chunks.estimated_orphans ?? '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.verified') }}</dt>
	              <dd>{{ maintenanceReport?.verify.verified_chunks ?? '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.verifyErrors') }}</dt>
	              <dd>{{ maintenanceReport ? maintenanceReport.verify.missing_index + maintenanceReport.verify.corrupt_chunks : '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.compacted') }}</dt>
	              <dd>{{ maintenanceReport ? t('summary.compacted', { chunks: maintenanceReport.compact.rewritten_chunks, bytes: formatBytes(maintenanceReport.compact.removed_segment_bytes) }) : '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.compactSkipped') }}</dt>
	              <dd>{{ compactSkippedText(maintenanceReport?.compact.skipped_reason) }}</dd>
	            </div>
	          </dl>
	        </section>
	      </section>

      <section v-else-if="activePage === 'settings'" class="view">
        <div class="two-column">
          <section class="panel">
            <div class="panel-title">
              <h2>{{ t('settings.repositoryDefaults') }}</h2>
              <span>{{ bootstrap?.repository.encryption ?? '-' }}</span>
            </div>
            <dl class="details">
              <div>
                <dt>{{ t('field.segment') }}</dt>
                <dd>{{ bootstrap?.repository.segment_size ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('field.chunk') }}</dt>
                <dd>{{ bootstrap?.repository.chunk_avg_size ?? '-' }}</dd>
              </div>
              <div>
                <dt>{{ t('field.compression') }}</dt>
                <dd>{{ bootstrap?.repository.compression ?? '-' }}</dd>
              </div>
            </dl>
          </section>
	          <section class="panel">
	            <div class="panel-title">
	              <h2>{{ t('settings.runtime') }}</h2>
	              <span>{{ bootstrap?.scheduler.timezone ?? '-' }}</span>
	            </div>
	            <form class="inline-form" @submit.prevent="saveSettings">
	              <label class="field">
	                <span>{{ t('field.adminUsername') }}</span>
	                <input v-model="settingsAdminUsername" type="text" required />
	              </label>
	              <label class="field">
	                <span>{{ t('field.sessionTTL') }}</span>
	                <input v-model.number="settingsSessionTTLHours" type="number" min="1" step="1" required />
	              </label>
	              <label class="field">
	                <span>{{ t('field.currentPassword') }}</span>
	                <input v-model="settingsCurrentPassword" type="password" autocomplete="current-password" />
	              </label>
	              <label class="field">
	                <span>{{ t('field.newPassword') }}</span>
	                <input v-model="settingsNewPassword" type="password" autocomplete="new-password" />
	              </label>
	              <label class="field">
	                <span>{{ t('field.keepLast') }}</span>
	                <input v-model.number="settingsKeepLast" type="number" min="1" step="1" required />
	              </label>
	              <label class="field">
	                <span>{{ t('field.daily') }}</span>
	                <input v-model.number="settingsKeepDaily" type="number" min="0" step="1" required />
	              </label>
	              <label class="field">
	                <span>{{ t('field.weekly') }}</span>
	                <input v-model.number="settingsKeepWeekly" type="number" min="0" step="1" required />
	              </label>
	              <button class="text-button primary" type="submit" :disabled="savingSettings">
	                <Save :size="16" />
	                <span>{{ savingSettings ? t('common.saving') : t('common.save') }}</span>
	              </button>
	            </form>
	          </section>
        </div>
      </section>
    </main>
  </div>
</template>
