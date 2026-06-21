<script setup lang="ts">
import { ArrowUp, Download, FolderOpen, RotateCcw } from '@lucide/vue';
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
</template>
