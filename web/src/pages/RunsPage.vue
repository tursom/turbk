<script setup lang="ts">
import { ScrollText } from '@lucide/vue';
import TablePanel from '../components/TablePanel';
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
</template>
