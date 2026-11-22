import { writable } from 'svelte/store';

// 界面主题：light / dark 固定配色；system 跟随操作系统（prefers-color-scheme）
export type ThemeMode = 'light' | 'dark' | 'system';

export const THEME_STORAGE_KEY = 'banbot-theme';
const MODES: ThemeMode[] = ['light', 'dark', 'system'];

export const themeMode = writable<ThemeMode>('system');

let current: ThemeMode = 'system';
let inited = false;

function readStoredMode(): ThemeMode {
  try {
    const v = localStorage.getItem(THEME_STORAGE_KEY);
    if (v && (MODES as string[]).includes(v)) return v as ThemeMode;
  } catch (e) {
    // 隐私模式或禁用存储时退回默认值
  }
  return 'system';
}

function resolve(mode: ThemeMode): 'light' | 'dark' {
  if (mode !== 'system') return mode;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function apply(mode: ThemeMode) {
  document.documentElement.setAttribute('data-theme', resolve(mode));
}

// 应用启动时调用一次（根布局）
export function initTheme() {
  if (inited || typeof window === 'undefined') return;
  inited = true;
  current = readStoredMode();
  themeMode.set(current);
  apply(current);
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (current === 'system') apply('system');
  });
}

export function setThemeMode(mode: ThemeMode) {
  current = mode;
  themeMode.set(mode);
  try {
    localStorage.setItem(THEME_STORAGE_KEY, mode);
  } catch (e) {
    // 存储不可用时仅本次生效
  }
  apply(mode);
}
