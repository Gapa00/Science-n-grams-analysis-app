import axios from 'axios';

const API_URL =
  process.env.REACT_APP_API_URL ||
  (process.env.NODE_ENV === 'production' ? '/api' : 'http://localhost:8000');

console.log('API URL:', API_URL);

const api = axios.create({
  baseURL: API_URL,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`, config.params || config.data);
    return config;
  },
  (error) => Promise.reject(error)
);

api.interceptors.response.use(
  (response) => {
    console.log(`API Response: ${response.status}`, response.data);
    return response;
  },
  (error) => {
    console.error('API Response Error:', {
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      message: error.message
    });
    return Promise.reject(error);
  }
);

// ---------- Types ----------

export type LoginResponse = { user_id: number; username: string };

export type NextPair =
  | { done: true; total: number; index?: null; left_id?: null; right_id?: null }
  | { done: false; total: number; index: number; left_id: number; right_id: number };

export type GetPairResponse = {
  total: number;
  index: number;
  left_id: number;
  right_id: number;
  choice?: 'left' | 'right';
};

export type FrequencyPoint = { date: string; count: number };
export type FrequencyApiResponse = { ngram_id: number; ngram_text: string; frequency_data: FrequencyPoint[] };

export type BurstBackendRow = {
  ngram_id: number;
  text: string;
  n_words: number;
  domain?: string | null;
  domain_id?: number | null;
  field?: string | null;
  field_id?: number | null;
  subfield?: string | null;
  subfield_id?: number | null;
  method: 'macd' | 'kleinberg';
  score: number;
  normalized_score: number;
  num_bursts?: number | null;
};

export type BurstBackendPagination = {
  page: number;
  page_size: number;
  total_count: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
};

export type BurstLeaderboardApiResponse = {
  data: BurstBackendRow[];
  pagination: BurstBackendPagination;
};

// ✅ UPDATED: Burst point data with full MACD metrics
export type BurstPoint = {
  date: string;
  period_index: number;
  contribution: number;  // Can be negative for MACD
  raw_value: number;
  baseline_value: number | null;
  
  // Complete MACD metrics (null for Kleinberg)
  macd_short_ema: number | null;
  macd_long_ema: number | null;
  macd_line: number | null;
  macd_signal: number | null;
  macd_histogram: number | null;
  
  // Kleinberg metrics (null for MACD)
  kleinberg_state: number | null;
  state_probability: number | null;
  weight_contribution: number | null;
};

export type BurstPointsApiResponse = {
  ngram_id: number;
  method: 'macd' | 'kleinberg';
  start: string | null;
  end: string | null;
  points: BurstPoint[];
};

// ---------- API Calls ----------

// --- Login API Call ---
export const login = async (username: string): Promise<LoginResponse> => {
  const { data } = await api.post('/v1/login', { username });
  return data;
};

// --- Binary Voting API Calls ---
export const getNextBinaryPair = async (user_id: number): Promise<NextPair> => {
  const { data } = await api.get('/v1/vote/binary/next', { params: { user_id } });
  return data;
};

export const getBinaryPair = async (user_id: number, index: number): Promise<GetPairResponse> => {
  const { data } = await api.get('/v1/vote/binary/pair', { params: { user_id, index } });
  return data;
};

export const submitBinaryVote = async (payload: {
  user_id: number;
  pair_index: number;
  left_id: number;
  right_id: number;
  choice: 'left' | 'right';
  rt_ms?: number;
}) => {
  const { data } = await api.post('/v1/vote/binary/submit', payload);
  return data as { ok: boolean };
};

// --- Slider Voting API Calls ---
export const submitSliderVote = async (payload: {
  user_id: number;
  ngram_id: number;
  slider_value: number;
}) => {
  const { data } = await api.post('/v1/vote/slider/submit', payload);
  return data as { ok: boolean };
};

export const getSliderData = async () => {
  const { data } = await api.get('/v1/vote/slider/data');
  return data;
};

// --- Frequency API Calls ---
export const getFrequency = async (ngramId: number): Promise<FrequencyApiResponse> => {
  const { data } = await api.get(`/v1/ngram/${ngramId}/frequency`);
  return data;
};

export type NgramDetails = {
  id: number;
  text: string;
  n_words: number;
  df_ngram: number;
  df_ngram_subfield: number;
  domain: string | null;
  domain_id: number | null;
  field: string | null;
  field_id: number | null;
  subfield: string | null;
  subfield_id: number | null;
};

export const getNgramDetails = async (ngramId: number): Promise<NgramDetails> => {
  const { data } = await api.get(`/v1/ngram/${ngramId}`);
  return data;
};

// ----------- Burst API Calls -----------

export const getBurstTimeBounds = async (): Promise<{ min: string | null; max: string | null }> => {
  const { data } = await api.get('/v1/bursts/time-bounds');
  return data;
};

export const getBurstLeaderboard = async (
  params: Record<string, any>
): Promise<BurstLeaderboardApiResponse> => {
  const { data } = await api.get('/v1/bursts/leaderboard', { params });
  return data;
};

// ✅ NEW: Get burst contribution points for visualization
export const getBurstPoints = async (params: {
  ngram_id: number;
  method: 'macd' | 'kleinberg';
  start?: string | null;
  end?: string | null;
  limit?: number;
}): Promise<BurstPointsApiResponse> => {
  const { data } = await api.get('/v1/bursts/points', { params });
  return data;
};

export default api;