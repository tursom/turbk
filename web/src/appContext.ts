import { inject, type InjectionKey } from 'vue';

export type AppContext = Record<string, any>;

export const appContextKey: InjectionKey<AppContext> = Symbol('turbk.appContext');

export function useAppContext() {
  const context = inject(appContextKey);
  if (!context) {
    throw new Error('Turbk app context is not available');
  }
  return context;
}
