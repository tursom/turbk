<script setup lang="ts">
import { Database, Shield, Wrench } from '@lucide/vue';
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
  maintenanceRuns,
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
	              <button class="text-button" type="button" :disabled="maintenanceRunning" @click="runMaintenance('cleanup-errors')">
	                <Wrench :size="16" />
	                <span>{{ t('common.cleanup') }}</span>
	              </button>
	              <button class="text-button" type="button" :disabled="maintenanceRunning" @click="runMaintenance('compact')">
	                <Database :size="16" />
	                <span>{{ t('common.compact') }}</span>
	              </button>
	              <button class="text-button primary" type="button" :disabled="maintenanceRunning" @click="runMaintenance('full-cleanup')">
	                <Database :size="16" />
	                <span>{{ t('common.fullCleanup') }}</span>
	              </button>
	            </div>
	          </div>
	          <dl class="details">
	            <div>
	              <dt>{{ t('field.mode') }}</dt>
	              <dd>{{ maintenanceModeText(maintenanceReport?.mode) }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.nextCleanup') }}</dt>
	              <dd>{{ nullText(storageHealth?.maintenance.next_cleanup_at) }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.nextCompact') }}</dt>
	              <dd>{{ nullText(storageHealth?.maintenance.next_compact_at) }}</dd>
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
	              <dt>{{ t('field.cleanedChunks') }}</dt>
	              <dd>{{ maintenanceReport?.cleanup.removed_chunks ?? '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.cleanedManifests') }}</dt>
	              <dd>{{ maintenanceReport?.cleanup.removed_manifests ?? '-' }}</dd>
	            </div>
	            <div>
	              <dt>{{ t('field.staleRuns') }}</dt>
	              <dd>{{ maintenanceReport?.cleanup.stale_runs_failed ?? '-' }}</dd>
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
	        <TablePanel :title="t('storage.maintenanceHistory')" :empty="maintenanceRuns.length === 0">
	          <thead>
	            <tr>
	              <th>{{ t('field.id') }}</th>
	              <th>{{ t('field.mode') }}</th>
	              <th>{{ t('field.status') }}</th>
	              <th>{{ t('field.started') }}</th>
	              <th>{{ t('field.finished') }}</th>
	              <th>{{ t('field.compactSkipped') }}</th>
	            </tr>
	          </thead>
	          <tbody>
	            <tr v-for="run in maintenanceRuns" :key="run.id">
	              <td>#{{ run.id }}</td>
	              <td>{{ maintenanceModeText(run.mode) }}</td>
	              <td><span class="tag">{{ statusText(run.status) }}</span></td>
	              <td>{{ formatTime(run.started_at) }}</td>
	              <td>{{ nullText(run.finished_at) }}</td>
	              <td>{{ compactSkippedText(nullText(run.skipped_reason)) }}</td>
	            </tr>
	          </tbody>
	        </TablePanel>
	      </section>
</template>
