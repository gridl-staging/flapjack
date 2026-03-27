import path from 'node:path';
import { getLocalInstanceConfig } from '../../local-instance-config';

const instance = getLocalInstanceConfig();

export const API_BASE = instance.backendBaseUrl;
export const BACKEND_DATA_DIR = instance.backendDataDir;
export const PERSONALIZATION_STRATEGY_PATH = path.join(BACKEND_DATA_DIR, 'personalization_strategy.json');
export const DASHBOARD_BASE = instance.dashboardBaseUrl;
export const TEST_ADMIN_KEY = instance.adminKey;

export const API_HEADERS = {
  'x-algolia-application-id': 'flapjack',
  'x-algolia-api-key': TEST_ADMIN_KEY,
  'Content-Type': 'application/json',
} as const;
