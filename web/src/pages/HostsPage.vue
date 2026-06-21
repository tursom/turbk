<script setup lang="ts">
import { Activity, Copy, HardDrive, Plus, Server, X } from '@lucide/vue';
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
  hostCredentialId,
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
  agentSetupMethod,
  agentSetupMethods,
  selectedAgentSetupSnippet,
  formatTime,
  formatBytes,
  formatPercent,
  runProgressText,
  nullText,
  yesNo,
  statusText,
  sourceTypeLabel,
  credentialsForSource,
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
  <div class="status-grid host-status-grid">
    <article v-for="row in hostSummaryRows" :key="row.label" class="stat-card">
      <component :is="row.icon" :size="20" />
      <div>
        <span>{{ row.label }}</span>
        <strong>{{ row.value }}</strong>
        <small>{{ row.detail }}</small>
      </div>
    </article>
  </div>

  <section v-if="showHostCreate" class="panel host-create-panel">
    <div class="panel-title">
      <div>
        <h2>{{ t('hosts.create') }}</h2>
        <p class="panel-description">{{ t('hosts.createIntro') }}</p>
      </div>
      <button class="icon-button" type="button" :title="t('common.close')" @click="showHostCreate = false">
        <X :size="18" />
      </button>
    </div>

    <div class="method-selector" role="listbox" :aria-label="t('hosts.connectionMethod')">
      <button
        v-for="option in hostSourceOptions"
        :key="option.value"
        class="method-option"
        :class="{ active: hostSourceType === option.value }"
        type="button"
        @click="hostSourceType = option.value"
      >
        <component :is="option.icon" :size="18" />
        <span>
          <strong>{{ option.title }}</strong>
          <small>{{ option.description }}</small>
        </span>
        <em>{{ option.label }}</em>
      </button>
    </div>

    <form class="host-create-form" @submit.prevent="createHost">
      <label class="field">
        <span>{{ t('field.name') }}</span>
        <input v-model="hostName" type="text" required />
      </label>
      <label v-if="!['agent', 'local'].includes(hostSourceType)" class="field">
        <span>{{ t('field.address') }}</span>
        <input v-model="hostAddress" type="text" :placeholder="hostAddressPlaceholder(hostSourceType)" />
      </label>
      <label v-if="!['agent', 'local'].includes(hostSourceType)" class="field">
        <span>{{ t('field.credential') }}</span>
        <select v-model="hostCredentialId" required>
          <option value="">{{ t('common.select') }}</option>
          <option v-for="credential in credentialsForSource(hostSourceType)" :key="credential.id" :value="credential.id">
            {{ credential.name }}
          </option>
        </select>
      </label>
      <div class="form-actions">
        <button class="text-button primary" type="submit">
          <Server :size="16" />
          <span>{{ t('common.create') }}</span>
        </button>
      </div>
    </form>
  </section>

  <div class="host-management-layout">
    <section class="panel host-list-panel">
      <div class="panel-title">
        <div>
          <h2>{{ t('nav.hosts') }}</h2>
          <p class="panel-description">{{ t('hosts.selectHost') }}</p>
        </div>
        <div class="panel-actions">
          <span v-if="hostActionMessage" class="action-note">{{ hostActionMessage }}</span>
          <button class="text-button primary" type="button" @click="openHostCreate('agent')">
            <Plus :size="16" />
            <span>{{ t('hosts.new') }}</span>
          </button>
        </div>
      </div>

      <div v-if="hosts.length === 0" class="empty-state">
        <span>{{ t('common.noRecords') }}</span>
        <button class="text-button primary" type="button" @click="openHostCreate('agent')">
          <Plus :size="16" />
          <span>{{ t('hosts.new') }}</span>
        </button>
      </div>
      <div v-else class="host-list">
        <button
          v-for="host in hosts"
          :key="host.id"
          class="host-list-item"
          :class="{ selected: selectedHost?.id === host.id }"
          type="button"
          @click="selectHost(host)"
        >
          <span class="host-list-main">
            <strong>{{ host.name }}</strong>
            <small>{{ sourceTypeLabel(host.source_type) }}</small>
          </span>
          <span class="host-list-meta">
            <span class="tag">{{ statusText(host.status) }}</span>
            <small>{{ hostAddressText(host) }}</small>
          </span>
        </button>
      </div>
    </section>

    <section class="panel host-detail-panel">
      <template v-if="selectedHost">
        <div class="panel-title">
          <div>
            <h2>{{ t('hosts.details') }}</h2>
            <p class="panel-description">#{{ selectedHost.id }} · {{ sourceTypeLabel(selectedHost.source_type) }}</p>
          </div>
          <span class="tag">{{ statusText(selectedHost.status) }}</span>
        </div>

        <div class="host-detail-head">
          <div class="host-avatar">
            <Activity v-if="selectedHost.source_type === 'agent'" :size="22" />
            <HardDrive v-else-if="selectedHost.source_type === 'local'" :size="22" />
            <Server v-else :size="22" />
          </div>
          <div>
            <strong>{{ selectedHost.name }}</strong>
            <span>{{ hostAddressText(selectedHost) }}</span>
          </div>
        </div>

        <dl class="details compact-details">
          <div>
            <dt>{{ t('field.address') }}</dt>
            <dd>{{ hostAddressText(selectedHost) }}</dd>
          </div>
          <div>
            <dt>{{ t('field.status') }}</dt>
            <dd>{{ statusText(selectedHost.status) }}</dd>
          </div>
          <div>
            <dt>{{ t('field.updated') }}</dt>
            <dd>{{ formatTime(selectedHost.updated_at) }}</dd>
          </div>
          <div v-if="selectedHost.source_type === 'agent'">
            <dt>{{ t('field.lastSeen') }}</dt>
            <dd>{{ hostLastSeenText(selectedHost) }}</dd>
          </div>
          <div>
            <dt>{{ t('field.created') }}</dt>
            <dd>{{ formatTime(selectedHost.created_at) }}</dd>
          </div>
        </dl>

        <div class="host-section">
          <h2>{{ t('hosts.connectionProfile') }}</h2>
          <p v-if="selectedHost.source_type === 'agent'" class="muted">{{ t('hosts.agentSetupDesc') }}</p>
          <p v-else-if="selectedHost.source_type === 'local'" class="muted">{{ t('hosts.localSetupDesc') }}</p>
          <p v-else class="muted">{{ t('hosts.pullSetupDesc') }}</p>
        </div>

        <div class="host-section">
          <h2>{{ t('hosts.boundCredential') }}</h2>
          <ul v-if="selectedHostCredentials.length > 0" class="compact-list">
            <li v-for="credential in selectedHostCredentials" :key="credential.id">
              <span>{{ credential.name }}</span>
              <code>{{ sourceTypeLabel(credential.type) }}</code>
            </li>
          </ul>
          <p v-else class="muted">{{ t('hosts.noBoundCredential') }}</p>
        </div>

        <div class="host-section">
          <h2>{{ t('hosts.managedJobs') }}</h2>
          <ul v-if="selectedHostJobs.length > 0" class="compact-list">
            <li v-for="job in selectedHostJobs" :key="job.id">
              <span>{{ job.name }}</span>
              <code>{{ sourceRoot(job) }}</code>
            </li>
          </ul>
          <p v-else class="muted">{{ t('hosts.noLinkedJobs') }}</p>
        </div>
      </template>
      <div v-else class="empty-state">
        <span>{{ t('hosts.noHostSelected') }}</span>
      </div>
    </section>
  </div>

  <section
    v-if="selectedHost?.source_type === 'agent' && ((agentCredentialClientId && agentCredentialSecret) || selectedAgentCredential)"
    class="panel agent-setup-panel"
  >
    <div class="panel-title">
      <div>
        <h2>{{ t('hosts.agentSetup') }}</h2>
        <p class="panel-description">{{ t('message.agentSecretStored') }}</p>
      </div>
      <span v-if="copyActionMessage" class="action-note">{{ copyActionMessage }}</span>
    </div>
    <p class="muted">{{ t('hosts.agentSetupDesc') }}</p>
    <dl class="details compact-details">
      <div>
        <dt>{{ t('field.clientID') }}</dt>
        <dd>{{ agentSetupClientId }}</dd>
      </div>
      <div>
        <dt>{{ t('field.clientSecret') }}</dt>
        <dd>{{ agentSetupClientSecret }}</dd>
      </div>
      <div>
        <dt>{{ t('hosts.serverURL') }}</dt>
        <dd>{{ currentServerURL }}</dd>
      </div>
    </dl>

    <div class="agent-setup-grid">
      <label class="field">
        <span>{{ t('hosts.sourceDir') }}</span>
        <input v-model="agentSetupSourceDir" type="text" />
      </label>
    </div>

    <div class="agent-method-layout">
      <div class="agent-method-selector" role="tablist" :aria-label="t('hosts.agentSetupMethod')">
        <button
          v-for="method in agentSetupMethods"
          :key="method.value"
          class="agent-method-option"
          :class="{ active: agentSetupMethod === method.value }"
          type="button"
          role="tab"
          :aria-selected="agentSetupMethod === method.value"
          @click="agentSetupMethod = method.value"
        >
          <strong>{{ method.title }}</strong>
          <small>{{ method.description }}</small>
        </button>
      </div>

      <div class="code-block agent-method-code">
        <div class="code-title">
          <span>{{ selectedAgentSetupSnippet.title }}</span>
          <button class="text-button" type="button" @click="copyText(selectedAgentSetupSnippet.code)">
            <Copy :size="16" />
            <span>{{ t('hosts.copySetup') }}</span>
          </button>
        </div>
        <p class="code-description">{{ selectedAgentSetupSnippet.description }}</p>
        <pre>{{ selectedAgentSetupSnippet.code }}</pre>
      </div>
    </div>
  </section>
</section>
</template>
