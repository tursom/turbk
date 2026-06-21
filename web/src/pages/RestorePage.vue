<script setup lang="ts">
import { RotateCcw } from '@lucide/vue';
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
</template>
