<script setup lang="ts">
import { provide } from 'vue';
import { Activity, CheckCircle2, LogOut, RefreshCw, Shield, User } from '@lucide/vue';
import { appContextKey } from './appContext';
import { useBackupApp } from './composables/useBackupApp';
import CredentialsPage from './pages/CredentialsPage.vue';
import DashboardPage from './pages/DashboardPage.vue';
import HostsPage from './pages/HostsPage.vue';
import JobsPage from './pages/JobsPage.vue';
import RestorePage from './pages/RestorePage.vue';
import RunsPage from './pages/RunsPage.vue';
import SettingsPage from './pages/SettingsPage.vue';
import SnapshotsPage from './pages/SnapshotsPage.vue';
import StoragePage from './pages/StoragePage.vue';

const app = useBackupApp();
provide(appContextKey, app);

const {
  t,
  locale,
  setLocale,
  checkingSession,
  authenticated,
  login,
  logout,
  loginUsername,
  loginPassword,
  loginLoading,
  loginError,
  pages,
  activePage,
  activeTitle,
  pagePath,
  navigatePage,
  currentUser,
  health,
  statusText,
  loading,
  refresh,
  error
} = app;
</script>

<template>
  <div v-if="checkingSession" class="login-shell">
    <section class="login-panel">
      <div class="login-topline">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div class="language-switch" role="group" :aria-label="t('language')">
          <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
          <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
        </div>
      </div>
      <h1>Turbk</h1>
      <div class="status-pill">
        <Activity :size="16" />
        <span>{{ t('common.loading') }}</span>
      </div>
    </section>
  </div>

  <div v-else-if="!authenticated" class="login-shell">
    <form class="login-panel" @submit.prevent="login">
      <div class="login-topline">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div class="language-switch" role="group" :aria-label="t('language')">
          <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
          <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
        </div>
      </div>
      <h1>Turbk</h1>
      <label class="field">
        <span>{{ t('auth.username') }}</span>
        <input v-model="loginUsername" type="text" autocomplete="username" required />
      </label>
      <label class="field">
        <span>{{ t('auth.password') }}</span>
        <input v-model="loginPassword" type="password" autocomplete="current-password" required />
      </label>
      <div v-if="loginError" class="error-bar">
        {{ loginError }}
      </div>
      <button class="text-button primary login-button" type="submit" :disabled="loginLoading">
        <User :size="16" />
        <span>{{ loginLoading ? t('auth.signingIn') : t('auth.signIn') }}</span>
      </button>
    </form>
  </div>

  <div v-else class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">
          <Shield :size="22" />
        </div>
        <div>
          <strong>Turbk</strong>
          <span>{{ t('brand.subtitle') }}</span>
        </div>
      </div>

      <nav class="nav-list" :aria-label="t('nav.primary')">
        <a
          v-for="page in pages"
          :key="page.key"
          class="nav-item"
          :class="{ active: activePage === page.key }"
          :href="pagePath(page.key)"
          @click.prevent="navigatePage(page.key)"
        >
          <component :is="page.icon" :size="18" />
          <span>{{ t(page.labelKey) }}</span>
        </a>
      </nav>
    </aside>

    <main class="workspace">
      <header class="topbar">
        <div>
          <p class="eyebrow">{{ activeTitle }}</p>
          <h1>{{ activeTitle }}</h1>
        </div>
        <div class="top-actions">
          <div class="language-switch" role="group" :aria-label="t('language')">
            <button type="button" :class="{ active: locale === 'zh' }" @click="setLocale('zh')">{{ t('language.zh') }}</button>
            <button type="button" :class="{ active: locale === 'en' }" @click="setLocale('en')">{{ t('language.en') }}</button>
          </div>
          <div class="user-chip">
            <User :size="16" />
            <span>{{ currentUser }}</span>
          </div>
          <div class="status-pill" :class="health?.status === 'ok' ? 'ok' : 'warn'">
            <CheckCircle2 v-if="health?.status === 'ok'" :size="16" />
            <Activity v-else :size="16" />
            <span>{{ statusText(health?.status ?? 'loading') }}</span>
          </div>
          <button class="icon-button" type="button" :title="t('common.refresh')" :disabled="loading" @click="refresh">
            <RefreshCw :class="{ spin: loading }" :size="18" />
          </button>
          <button class="icon-button" type="button" :title="t('common.logout')" @click="logout">
            <LogOut :size="18" />
          </button>
        </div>
      </header>

      <div v-if="error" class="error-bar">
        {{ error }}
      </div>

      <DashboardPage v-if="activePage === 'dashboard'" />
      <HostsPage v-else-if="activePage === 'hosts'" />
      <CredentialsPage v-else-if="activePage === 'credentials'" />
      <JobsPage v-else-if="activePage === 'jobs'" />
      <RunsPage v-else-if="activePage === 'runs'" />
      <SnapshotsPage v-else-if="activePage === 'snapshots'" />
      <RestorePage v-else-if="activePage === 'restore'" />
      <StoragePage v-else-if="activePage === 'storage'" />
      <SettingsPage v-else-if="activePage === 'settings'" />
    </main>
  </div>
</template>
