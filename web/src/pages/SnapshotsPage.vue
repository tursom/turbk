<script setup lang="ts">
import { ArrowUp, Download, FolderOpen, RotateCcw, Trash2 } from '@lucide/vue';
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
  selectedSnapshotIds,
  snapshotDeleteTarget,
  snapshotDeleteMany,
  deletingSnapshots,
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
} = useAppContext();
</script>

<template>
  <section class="view">
  <TablePanel :title="t('nav.snapshots')" :empty="snapshots.length === 0">
    <template #actions>
      <button class="text-button" type="button" :disabled="selectedSnapshotIds.length === 0" @click="requestDeleteSelectedSnapshots">
        <Trash2 :size="16" />
        <span>{{ t('common.delete') }} {{ selectedSnapshotIds.length }}</span>
      </button>
    </template>
    <thead>
      <tr>
        <th>
          <input
            type="checkbox"
            :checked="snapshots.length > 0 && selectedSnapshotIds.length === snapshots.length"
            :indeterminate.prop="selectedSnapshotIds.length > 0 && selectedSnapshotIds.length < snapshots.length"
            @change="toggleAllSnapshots(($event.target as HTMLInputElement).checked)"
          />
        </th>
        <th>{{ t('field.id') }}</th>
        <th>{{ t('field.job') }}</th>
        <th>{{ t('field.manifest') }}</th>
        <th>{{ t('field.source') }}</th>
        <th>{{ t('field.files') }}</th>
        <th>{{ t('field.size') }}</th>
        <th>{{ t('field.health') }}</th>
        <th>{{ t('field.created') }}</th>
        <th>{{ t('field.action') }}</th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="snapshot in snapshots" :key="snapshot.id">
        <td>
          <input
            type="checkbox"
            :checked="selectedSnapshotIds.includes(snapshot.id)"
            @change="toggleSnapshotSelection(snapshot, ($event.target as HTMLInputElement).checked)"
          />
        </td>
        <td>#{{ snapshot.id }}</td>
        <td>{{ nullText(snapshot.job_id) }}</td>
        <td>{{ snapshot.manifest_ref }}</td>
        <td>{{ sourceTypeLabel(snapshot.source_type) }}</td>
        <td>{{ snapshot.file_count }}</td>
        <td>{{ formatBytes(snapshot.total_size) }}</td>
        <td><span class="tag">{{ statusText(snapshot.health || 'unknown') }}</span></td>
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
            <button class="text-button danger" type="button" @click="requestDeleteSnapshot(snapshot)">
              <Trash2 :size="16" />
              <span>{{ t('common.delete') }}</span>
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
  <div v-if="snapshotDeleteTarget || snapshotDeleteMany" class="drawer-backdrop" @click.self="cancelSnapshotDelete">
    <aside class="drawer-panel compact-dialog" aria-modal="true" role="dialog">
      <div class="panel-title">
        <h2>{{ t('snapshots.deleteTitle') }}</h2>
        <button class="icon-button" type="button" @click="cancelSnapshotDelete">×</button>
      </div>
      <p class="panel-description">
        {{
          snapshotDeleteMany
            ? t('snapshots.deleteManyConfirm', { count: selectedSnapshotIds.length })
            : t('snapshots.deleteOneConfirm', { id: snapshotDeleteTarget?.id ?? '-' })
        }}
      </p>
      <p class="panel-description">{{ t('snapshots.deleteSpaceHint') }}</p>
      <div class="button-row dialog-actions">
        <button class="text-button" type="button" @click="cancelSnapshotDelete">
          <span>{{ t('common.cancel') }}</span>
        </button>
        <button class="text-button danger" type="button" :disabled="deletingSnapshots" @click="confirmSnapshotDelete">
          <Trash2 :size="16" />
          <span>{{ deletingSnapshots ? t('common.running') : t('common.delete') }}</span>
        </button>
      </div>
    </aside>
  </div>
</section>
</template>
