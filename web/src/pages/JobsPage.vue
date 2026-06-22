<script setup lang="ts">
import { CalendarClock, Pencil, Play, Plus, Power, Save, X } from '@lucide/vue';
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
  savingSettings,
  statRows,
  hostSummaryRows,
  jobHostOptions,
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
  credentialsForSource,
  jobHostLabel,
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
        <th>{{ t('nav.hosts') }}</th>
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
        <td>{{ jobHostLabel(job) }}</td>
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
            <button class="text-button" type="button" :disabled="runningJobId === job.id" @click="runJob(job)">
              <Play :size="16" />
              <span>{{ runningJobId === job.id ? t('common.running') : t('common.run') }}</span>
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
        <span>{{ t('nav.hosts') }}</span>
        <select v-model="jobHostId" required>
          <option value="">{{ t('common.select') }}</option>
          <option v-for="host in jobHostOptions" :key="host.id" :value="host.id">
            {{ host.name }} · {{ sourceTypeLabel(host.source_type) }}
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
</template>
