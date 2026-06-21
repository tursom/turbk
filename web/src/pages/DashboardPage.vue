<script setup lang="ts">
import { Activity, Database, Server, UploadCloud } from '@lucide/vue';
import { useAppContext } from '../appContext';

const {
  t,
  counts,
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
  agentCredentialClientId,
  agentCredentialSecret,
  jobName,
  jobSourceType,
  jobRoot,
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
  statRows,
  hostSummaryRows,
  hostSourceOptions,
  selectedHost,
  selectedHostCredentials,
  selectedHostJobs,
  selectedAgentCredential,
  currentServerURL,
  agentSetupClientId,
  agentSetupClientSecret,
  agentComposeEnv,
  agentDockerCommand,
  formatTime,
  formatBytes,
  formatPercent,
  runProgressText,
  nullText,
  yesNo,
  statusText,
  sourceTypeLabel,
  entryTypeLabel,
  phaseText,
  maintenanceModeText,
  compactSkippedText,
  sourceRoot,
  selectHost,
  openHostCreate,
  hostAddressText,
  hostLastSeenText,
  hostAddressPlaceholder,
  copyText,
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
} = useAppContext();
</script>

<template>
  <section class="view">
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
</template>
