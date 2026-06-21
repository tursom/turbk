<script setup lang="ts">
import { KeyRound, Plus, X } from '@lucide/vue';
import { useAppContext } from '../appContext';

const {
  t,
  credentials,
  credentialName,
  credentialType,
  credentialUsername,
  credentialPassword,
  credentialPrivateKey,
  credentialBearerToken,
  credentialExplicitTLS,
  credentialSkipTLSVerify,
  showCredentialCreate,
  actionMessage,
  credentialSourceOptions,
  formatTime,
  sourceTypeLabel,
  credentialUsageText,
  openCredentialCreate,
  createCredential
} = useAppContext();
</script>

<template>
  <section class="view">
    <section class="panel credential-list-panel full-list-panel">
      <div class="panel-title">
        <div>
          <h2>{{ t('nav.credentials') }}</h2>
          <p class="panel-description">{{ t('credentials.selectCredential') }}</p>
        </div>
        <div class="panel-actions">
          <span v-if="actionMessage" class="action-note">{{ actionMessage }}</span>
          <button class="text-button primary" type="button" @click="openCredentialCreate('sftp')">
            <Plus :size="16" />
            <span>{{ t('credentials.new') }}</span>
          </button>
        </div>
      </div>

      <div v-if="credentials.length === 0" class="empty-state">
        <span>{{ t('common.noRecords') }}</span>
        <button class="text-button primary" type="button" @click="openCredentialCreate('sftp')">
          <Plus :size="16" />
          <span>{{ t('credentials.new') }}</span>
        </button>
      </div>
      <div v-else class="credential-list">
        <article v-for="credential in credentials" :key="credential.id" class="credential-list-item static-list-item">
          <span class="host-list-main">
            <strong>{{ credential.name }}</strong>
            <small>#{{ credential.id }} · {{ t('credentials.secretMaterial') }}</small>
          </span>
          <span class="host-list-meta">
            <span class="tag">{{ sourceTypeLabel(credential.type) }}</span>
            <small>{{ credentialUsageText(credential) }}</small>
            <small>{{ formatTime(credential.updated_at) }}</small>
          </span>
        </article>
      </div>
    </section>

    <div v-if="showCredentialCreate" class="drawer-backdrop" @click.self="showCredentialCreate = false">
      <aside class="drawer-panel" aria-modal="true" role="dialog">
        <div class="panel-title">
          <div>
            <h2>{{ t('credentials.create') }}</h2>
            <p class="panel-description">{{ t('credentials.createIntro') }}</p>
          </div>
          <button class="icon-button" type="button" :title="t('common.close')" @click="showCredentialCreate = false">
            <X :size="18" />
          </button>
        </div>

        <div class="method-selector drawer-method-selector" role="listbox" :aria-label="t('credentials.method')">
          <button
            v-for="option in credentialSourceOptions"
            :key="option.value"
            class="method-option"
            :class="{ active: credentialType === option.value }"
            type="button"
            @click="credentialType = option.value"
          >
            <component :is="option.icon" :size="18" />
            <span>
              <strong>{{ option.title }}</strong>
              <small>{{ option.description }}</small>
            </span>
            <em>{{ option.label }}</em>
          </button>
        </div>

        <form class="drawer-form" @submit.prevent="createCredential">
          <label class="field">
            <span>{{ t('field.name') }}</span>
            <input v-model="credentialName" type="text" required />
          </label>
          <label class="field">
            <span>{{ t('auth.username') }}</span>
            <input v-model="credentialUsername" type="text" :required="credentialType !== 'webdav'" />
          </label>
          <label class="field">
            <span>{{ t('auth.password') }}</span>
            <input v-model="credentialPassword" type="password" />
          </label>
          <label v-if="credentialType === 'sftp'" class="field">
            <span>{{ t('field.privateKey') }}</span>
            <textarea v-model="credentialPrivateKey" rows="5"></textarea>
          </label>
          <label v-if="credentialType === 'webdav'" class="field">
            <span>{{ t('field.bearerToken') }}</span>
            <input v-model="credentialBearerToken" type="password" />
          </label>
          <label v-if="credentialType === 'ftps'" class="field checkbox-field">
            <input v-model="credentialExplicitTLS" type="checkbox" />
            <span>{{ t('field.explicitTLS') }}</span>
          </label>
          <label v-if="credentialType === 'ftps'" class="field checkbox-field">
            <input v-model="credentialSkipTLSVerify" type="checkbox" />
            <span>{{ t('field.skipTLSVerify') }}</span>
          </label>
          <div class="form-actions">
            <button class="text-button primary" type="submit">
              <KeyRound :size="16" />
              <span>{{ t('common.create') }}</span>
            </button>
          </div>
        </form>
      </aside>
    </div>
  </section>
</template>
