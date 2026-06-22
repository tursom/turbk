<script setup lang="ts">
import { Activity, CalendarClock, Copy, HardDrive, KeyRound, Play, Plus, Search, Server, X } from '@lucide/vue';
import { useAppContext } from '../appContext';

const {
  t,
  hosts,
  filteredHosts,
  hostSummaryRows,
  hostSourceOptions,
  hostSourceFilterOptions,
  hostName,
  hostSourceType,
  hostAddress,
  hostCredentialId,
  showHostCreate,
  hostActionMessage,
  activeHostTab,
  hostSearch,
  hostSourceFilter,
  agentSetupSourceDirs,
  agentSetupRunMode,
  copyActionMessage,
  agentCredentialClientId,
  agentCredentialSecret,
  runningJobId,
  selectedHost,
  selectedHostCredentials,
  selectedHostJobs,
  selectedAgentCredential,
  currentServerURL,
  agentSetupClientId,
  agentSetupClientSecret,
  agentContainerStateDir,
  agentSetupRoots,
  agentSetupRootErrors,
  agentSetupReady,
  agentSetupStateMount,
  agentSetupSourceMounts,
  agentSetupSourceMount,
  agentSetupMethod,
  agentSetupMethods,
  agentRunModeOptions,
  hostTabOptions,
  selectedAgentSetupSnippet,
  addAgentSetupSourceDir,
  removeAgentSetupSourceDir,
  formatTime,
  nullText,
  yesNo,
  statusText,
  sourceTypeLabel,
  credentialsForSource,
  sourceRoot,
  selectHost,
  setActiveHostTab,
  openHostCreate,
  hostAddressText,
  hostLastSeenText,
  hostAddressPlaceholder,
  copyText,
  createHost,
  runJob
} = useAppContext();
</script>

<template>
  <section class="view hosts-workbench-view">
    <section class="panel hosts-toolbar">
      <div>
        <h2>{{ t('nav.hosts') }}</h2>
        <p class="panel-description">{{ t('hosts.selectHost') }}</p>
      </div>
      <div class="hosts-toolbar-metrics">
        <span v-for="row in hostSummaryRows.slice(0, 3)" :key="row.label" class="toolbar-metric">
          <strong>{{ row.value }}</strong>
          <small>{{ row.label }}</small>
        </span>
      </div>
      <div class="panel-actions">
        <span v-if="hostActionMessage" class="action-note">{{ hostActionMessage }}</span>
        <button class="text-button primary" type="button" @click="openHostCreate('agent')">
          <Plus :size="16" />
          <span>{{ t('hosts.new') }}</span>
        </button>
      </div>
    </section>

    <div class="host-workbench-layout">
      <section class="panel host-directory-panel">
        <div class="host-directory-tools">
          <label class="field host-search-field">
            <span>{{ t('hosts.search') }}</span>
            <span class="input-with-icon">
              <Search :size="16" />
              <input v-model="hostSearch" type="text" :placeholder="t('hosts.searchPlaceholder')" />
            </span>
          </label>
          <label class="field">
            <span>{{ t('hosts.sourceFilter') }}</span>
            <select v-model="hostSourceFilter">
              <option v-for="option in hostSourceFilterOptions" :key="option.value" :value="option.value">
                {{ option.label }}
              </option>
            </select>
          </label>
        </div>

        <div v-if="hosts.length === 0" class="empty-state compact-empty">
          <span>{{ t('common.noRecords') }}</span>
          <button class="text-button primary" type="button" @click="openHostCreate('agent')">
            <Plus :size="16" />
            <span>{{ t('hosts.new') }}</span>
          </button>
        </div>
        <div v-else-if="filteredHosts.length === 0" class="empty-state compact-empty">
          <span>{{ t('hosts.noMatchingHosts') }}</span>
        </div>
        <div v-else class="host-list">
          <button
            v-for="host in filteredHosts"
            :key="host.id"
            class="host-list-item"
            :class="{ selected: selectedHost?.id === host.id }"
            type="button"
            @click="selectHost(host)"
          >
            <span class="host-list-main">
              <strong>{{ host.name }}</strong>
              <small>#{{ host.id }} · {{ sourceTypeLabel(host.source_type) }}</small>
            </span>
            <span class="host-list-meta">
              <span class="tag">{{ statusText(host.status) }}</span>
              <small>{{ host.source_type === 'agent' ? hostLastSeenText(host) : hostAddressText(host) }}</small>
            </span>
          </button>
        </div>
      </section>

      <section class="panel host-detail-panel host-workbench-detail">
        <template v-if="selectedHost">
          <div class="host-detail-top">
            <div class="host-detail-head compact">
              <div class="host-avatar">
                <Activity v-if="selectedHost.source_type === 'agent'" :size="22" />
                <HardDrive v-else-if="selectedHost.source_type === 'local'" :size="22" />
                <Server v-else :size="22" />
              </div>
              <div>
                <strong>{{ selectedHost.name }}</strong>
                <span>#{{ selectedHost.id }} · {{ sourceTypeLabel(selectedHost.source_type) }}</span>
              </div>
            </div>
            <div class="host-detail-status">
              <span class="tag">{{ statusText(selectedHost.status) }}</span>
              <small>{{ t('field.updated') }} {{ formatTime(selectedHost.updated_at) }}</small>
            </div>
          </div>

          <div class="host-tabs" role="tablist" :aria-label="t('hosts.details')">
            <button
              v-for="tab in hostTabOptions"
              :key="tab.value"
              class="host-tab-button"
              :class="{ active: activeHostTab === tab.value }"
              type="button"
              role="tab"
              :aria-selected="activeHostTab === tab.value"
              @click="setActiveHostTab(tab.value)"
            >
              {{ tab.label }}
            </button>
          </div>

          <div v-if="activeHostTab === 'overview'" class="host-tab-panel">
            <div class="host-info-grid">
              <section class="host-info-section">
                <h3>{{ t('hosts.connectionProfile') }}</h3>
                <dl class="details compact-details">
                  <div>
                    <dt>{{ t('field.address') }}</dt>
                    <dd>{{ hostAddressText(selectedHost) }}</dd>
                  </div>
                  <div>
                    <dt>{{ t('field.status') }}</dt>
                    <dd>{{ statusText(selectedHost.status) }}</dd>
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
              </section>

              <section class="host-info-section">
                <h3>{{ t('hosts.boundCredential') }}</h3>
                <ul v-if="selectedHostCredentials.length > 0" class="compact-list">
                  <li v-for="credential in selectedHostCredentials" :key="credential.id">
                    <span>{{ credential.name }}</span>
                    <code>{{ sourceTypeLabel(credential.type) }}</code>
                  </li>
                </ul>
                <p v-else class="muted">{{ t('hosts.noBoundCredential') }}</p>
              </section>

              <section class="host-info-section">
                <h3>{{ t('hosts.managedJobs') }}</h3>
                <p class="metric-line">
                  <CalendarClock :size="16" />
                  <span>{{ selectedHostJobs.length }} {{ t('nav.jobs') }}</span>
                </p>
                <p v-if="selectedHost.source_type === 'agent'" class="muted">{{ t('hosts.agentSetupDesc') }}</p>
                <p v-else-if="selectedHost.source_type === 'local'" class="muted">{{ t('hosts.localSetupDesc') }}</p>
                <p v-else class="muted">{{ t('hosts.pullSetupDesc') }}</p>
              </section>
            </div>
          </div>

          <div v-else-if="activeHostTab === 'access'" class="host-tab-panel">
            <template v-if="selectedHost.source_type === 'agent'">
              <section class="host-info-section">
                <div class="section-title-row">
                  <div>
                    <h3>{{ t('hosts.agentSetup') }}</h3>
                    <p class="panel-description">{{ t('message.agentSecretStored') }}</p>
                  </div>
                  <span v-if="copyActionMessage" class="action-note">{{ copyActionMessage }}</span>
                </div>
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
              </section>

              <div class="agent-access-layout">
                <section class="agent-access-section">
                  <h3>{{ t('hosts.agentDaemonStatus') }}</h3>
                  <dl class="details compact-details">
                    <div>
                      <dt>{{ t('field.status') }}</dt>
                      <dd>
                        {{
                          nullText(selectedHost.agent_status?.running_run_id) !== '-'
                            ? t('common.running')
                            : nullText(selectedHost.agent_status?.last_seen_at) !== '-'
                              ? t('status.online')
                              : t('status.offline')
                        }}
                      </dd>
                    </div>
                    <div>
                      <dt>{{ t('field.lastSeen') }}</dt>
                      <dd>{{ hostLastSeenText(selectedHost) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('field.mode') }}</dt>
                      <dd>{{ selectedHost.agent_status?.mode || t('hosts.waitingHeartbeat') }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentRunningRun') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.running_run_id) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentLastError') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.last_error) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentLastDropped') }}</dt>
                      <dd>
                        {{ nullText(selectedHost.agent_status?.last_dropped_reason) }}
                        <small v-if="nullText(selectedHost.agent_status?.last_dropped_at) !== '-'">
                          {{ nullText(selectedHost.agent_status?.last_dropped_at) }}
                        </small>
                      </dd>
                    </div>
                  </dl>
                </section>

                <section class="agent-access-section">
                  <h3>{{ t('hosts.agentCatalogSync') }}</h3>
                  <dl class="details compact-details">
                    <div>
                      <dt>{{ t('hosts.agentCatalog') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.catalog_status) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentLocalDB') }}</dt>
                      <dd>
                        {{ nullText(selectedHost.agent_status?.catalog_status) === 'ok' ? yesNo(true) : nullText(selectedHost.agent_status?.catalog_status) === '-' ? '-' : yesNo(false) }}
                      </dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentRepository') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.repository_id) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentChunkGeneration') }}</dt>
                      <dd>{{ selectedHost.agent_status?.chunk_generation ?? '-' }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentLastSync') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.last_seen_at) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentStateDir') }}</dt>
                      <dd>{{ nullText(selectedHost.agent_status?.state_dir) }}</dd>
                    </div>
                  </dl>
                </section>

                <section class="agent-access-section">
                  <h3>{{ t('hosts.agentStorageMapping') }}</h3>
                  <dl class="details compact-details">
                    <div>
                      <dt>{{ t('hosts.agentStateDir') }}</dt>
                      <dd><code>{{ agentContainerStateDir }}</code></dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentStateMount') }}</dt>
                      <dd><code>{{ agentSetupStateMount }}</code></dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentRoots') }}</dt>
                      <dd class="path-stack">
                        <code v-for="root in agentSetupRoots" :key="root">{{ root }}</code>
                      </dd>
                    </div>
                    <div>
                      <dt>{{ t('hosts.agentSourceMount') }}</dt>
                      <dd class="path-stack">
                        <code v-for="mount in agentSetupSourceMounts" :key="mount">{{ mount }}</code>
                      </dd>
                    </div>
                  </dl>
                </section>
              </div>

              <div class="agent-setup-grid">
                <div class="field agent-run-mode-field">
                  <span>{{ t('hosts.agentRunMode') }}</span>
                  <div class="agent-run-mode-selector" role="tablist" :aria-label="t('hosts.agentRunMode')">
                    <button
                      v-for="mode in agentRunModeOptions"
                      :key="mode.value"
                      class="agent-run-mode-option"
                      :class="{ active: agentSetupRunMode === mode.value }"
                      type="button"
                      role="tab"
                      :aria-selected="agentSetupRunMode === mode.value"
                      @click="agentSetupRunMode = mode.value"
                    >
                      <strong>{{ mode.label }}</strong>
                      <small>{{ mode.description }}</small>
                    </button>
                  </div>
                </div>
                <div class="field wide agent-root-list">
                  <span>{{ t('hosts.sourceDirs') }}</span>
                  <div v-for="(_, index) in agentSetupSourceDirs" :key="index" class="agent-root-row">
                    <input v-model="agentSetupSourceDirs[index]" type="text" :placeholder="t('hosts.sourceDirPlaceholder')" />
                    <button class="icon-button" type="button" :disabled="agentSetupSourceDirs.length <= 1" @click="removeAgentSetupSourceDir(index)">
                      <X :size="16" />
                    </button>
                  </div>
                  <button class="text-button" type="button" @click="addAgentSetupSourceDir">
                    <Plus :size="16" />
                    <span>{{ t('hosts.addSourceDir') }}</span>
                  </button>
                  <small v-for="message in agentSetupRootErrors" :key="message" class="field-error">{{ message }}</small>
                </div>
              </div>
              <p class="muted agent-access-note">{{ t('hosts.agentMountHint') }}</p>

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
                    <button class="text-button" type="button" :disabled="!agentSetupReady" @click="copyText(selectedAgentSetupSnippet.code)">
                      <Copy :size="16" />
                      <span>{{ t('hosts.copySetup') }}</span>
                    </button>
                  </div>
                  <p class="code-description">{{ selectedAgentSetupSnippet.description }}</p>
                  <p v-if="!agentSetupReady" class="code-warning">{{ t('hosts.agentSetupFixRoots') }}</p>
                  <pre>{{ selectedAgentSetupSnippet.code }}</pre>
                </div>
              </div>
            </template>

            <template v-else>
              <div class="host-info-grid">
                <section class="host-info-section">
                  <h3>{{ t('hosts.connectionProfile') }}</h3>
                  <dl class="details compact-details">
                    <div>
                      <dt>{{ t('field.address') }}</dt>
                      <dd>{{ hostAddressText(selectedHost) }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('field.credential') }}</dt>
                      <dd>{{ selectedHostCredentials[0]?.name ?? '-' }}</dd>
                    </div>
                    <div>
                      <dt>{{ t('field.status') }}</dt>
                      <dd>{{ statusText(selectedHost.status) }}</dd>
                    </div>
                  </dl>
                </section>
                <section class="host-info-section">
                  <h3>{{ t('hosts.boundCredential') }}</h3>
                  <ul v-if="selectedHostCredentials.length > 0" class="compact-list">
                    <li v-for="credential in selectedHostCredentials" :key="credential.id">
                      <span>{{ credential.name }}</span>
                      <code>{{ sourceTypeLabel(credential.type) }}</code>
                    </li>
                  </ul>
                  <p v-else class="muted">{{ t('hosts.noBoundCredential') }}</p>
                </section>
              </div>
            </template>
          </div>

          <div v-else class="host-tab-panel">
            <div v-if="selectedHostJobs.length === 0" class="empty-state compact-empty">
              <span>{{ t('hosts.noLinkedJobs') }}</span>
            </div>
            <div v-else class="table-wrap">
              <table class="compact-table">
                <thead>
                  <tr>
                    <th>{{ t('field.name') }}</th>
                    <th>{{ t('field.root') }}</th>
                    <th>{{ t('field.schedule') }}</th>
                    <th>{{ t('field.status') }}</th>
                    <th>{{ t('common.run') }}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="job in selectedHostJobs" :key="job.id">
                    <td>{{ job.name }}</td>
                    <td><code>{{ sourceRoot(job) }}</code></td>
                    <td>{{ nullText(job.schedule) }}</td>
                    <td>{{ job.enabled ? t('status.enabled') : t('status.disabled') }}</td>
                    <td>
                      <button class="icon-button" type="button" :disabled="runningJobId === job.id" @click="runJob(job)">
                        <Play :size="16" />
                      </button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </template>
        <div v-else class="empty-state">
          <span>{{ t('hosts.noHostSelected') }}</span>
        </div>
      </section>
    </div>

    <div v-if="showHostCreate" class="drawer-backdrop" @click.self="showHostCreate = false">
      <aside class="drawer-panel" aria-modal="true" role="dialog">
        <div class="panel-title">
          <div>
            <h2>{{ t('hosts.create') }}</h2>
            <p class="panel-description">{{ t('hosts.createIntro') }}</p>
          </div>
          <button class="icon-button" type="button" :title="t('common.close')" @click="showHostCreate = false">
            <X :size="18" />
          </button>
        </div>

        <div class="method-selector drawer-method-selector" role="listbox" :aria-label="t('hosts.connectionMethod')">
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

        <form class="drawer-form" @submit.prevent="createHost">
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
              <KeyRound v-if="hostSourceType === 'agent'" :size="16" />
              <Server v-else :size="16" />
              <span>{{ t('common.create') }}</span>
            </button>
          </div>
        </form>
      </aside>
    </div>
  </section>
</template>
