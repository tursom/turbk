<script setup lang="ts">
import { Save } from '@lucide/vue';
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
  copyActionMessage,
  credentialName,
  credentialType,
  credentialUsername,
  credentialPassword,
  credentialPrivateKey,
  credentialBearerToken,
  credentialExplicitTLS,
  credentialSkipTLSVerify,
  agentCredentialClientId,
  agentCredentialSecret,
  jobName,
  jobRoot,
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
</template>
