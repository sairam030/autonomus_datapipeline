/**
 * API client for communicating with the Autonomous Pipeline backend.
 */

import axios from 'axios';

const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE,
  headers: { 'Content-Type': 'application/json' },
});

// ============================================================================
// Types
// ============================================================================

export type SourceType = 'csv' | 'json' | 'parquet' | 'api' | 'kafka' | 'database';

export interface Pipeline {
  id: string;
  name: string;
  description: string | null;
  source_type: SourceType;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface FieldSchema {
  name: string;
  detected_type: string;
  nullable: boolean;
  confidence: number;
  sample_values: any[];
  unique_count: number;
  null_count: number;
  total_count: number;
  min_value: any;
  max_value: any;
}

export interface FileInfo {
  path: string;
  filename: string;
  size_bytes: number;
  row_count: number | null;
  schema_match: boolean;
  mismatch_reason: string | null;
}

export interface SchemaDetectionResult {
  pipeline_id: string;
  schema_id: string | null;
  schema_version: number;
  fields: FieldSchema[];
  total_files: number;
  compatible_files: FileInfo[];
  incompatible_files: FileInfo[];
  sample_row_count: number;
  detection_confidence: number;
  detected_at: string;
}

export interface SchemaConfirmResponse {
  pipeline_id: string;
  schema_id: string;
  schema_version: number;
  status: string;
  bronze_path: string;
  total_files_to_ingest: number;
  message: string;
}

export interface BronzeIngestionStatus {
  ingestion_id: string;
  pipeline_id: string;
  status: string;
  total_records: number;
  files_processed: number;
  files_skipped: number;
  duration_seconds: number;
  error_message: string | null;
}

// ============================================================================
// Pipeline API
// ============================================================================

export const pipelineApi = {
  create: (data: { name: string; description?: string; source_type: SourceType }) =>
    api.post<Pipeline>('/api/pipelines/', data).then(r => r.data),

  list: (filters?: { status?: string; source_type?: string }) =>
    api.get<Pipeline[]>('/api/pipelines/', { params: filters }).then(r => r.data),

  get: (id: string) =>
    api.get<Pipeline>(`/api/pipelines/${id}`).then(r => r.data),

  update: (id: string, data: Partial<Pipeline>) =>
    api.put<Pipeline>(`/api/pipelines/${id}`, data).then(r => r.data),

  delete: (id: string) =>
    api.delete(`/api/pipelines/${id}`),

  configureSource: (pipelineId: string, sourceConfig: any) =>
    api.post(`/api/pipelines/${pipelineId}/source`, sourceConfig).then(r => r.data),
};

// ============================================================================
// Upload API
// ============================================================================

export interface UploadResult {
  pipeline_id: string;
  upload_dir: string;
  uploaded_files: { filename: string; path: string; size_bytes: number }[];
  errors: { filename: string; error: string }[];
  total_uploaded: number;
  total_errors: number;
  detected_format: string | null;
}

export const uploadApi = {
  uploadFiles: (pipelineId: string, files: File[]) => {
    const formData = new FormData();
    files.forEach(f => formData.append('files', f));
    return api.post<UploadResult>(`/api/upload/${pipelineId}/files`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }).then(r => r.data);
  },

  listFiles: (pipelineId: string) =>
    api.get(`/api/upload/${pipelineId}/files`).then(r => r.data),

  deleteFile: (pipelineId: string, filename: string) =>
    api.delete(`/api/upload/${pipelineId}/files/${filename}`).then(r => r.data),

  deleteAll: (pipelineId: string) =>
    api.delete(`/api/upload/${pipelineId}/files`).then(r => r.data),
};

// ============================================================================
// Schema API
// ============================================================================

export const schemaApi = {
  detect: (pipelineId: string) =>
    api.post<SchemaDetectionResult>(`/api/schemas/detect?pipeline_id=${pipelineId}`).then(r => r.data),

  confirm: (data: {
    pipeline_id: string;
    schema_id: string;
    field_overrides?: any[];
    exclude_files?: string[];
    include_files?: string[];
  }) =>
    api.post<SchemaConfirmResponse>('/api/schemas/confirm', data).then(r => r.data),

  listVersions: (pipelineId: string) =>
    api.get(`/api/schemas/${pipelineId}/versions`).then(r => r.data),
};

// ============================================================================
// Bronze API
// ============================================================================

export const bronzeApi = {
  listIngestions: (pipelineId: string) =>
    api.get(`/api/bronze/${pipelineId}/ingestions`).then(r => r.data),

  getIngestionStatus: (ingestionId: string) =>
    api.get<BronzeIngestionStatus>(`/api/bronze/ingestion/${ingestionId}/status`).then(r => r.data),
};

// ============================================================================
// DAG API
// ============================================================================

export interface DAGScheduleConfig {
  schedule: string | null;
  start_date: string;
  retries: number;
  retry_delay_min: number;
  owner: string;
  source_type?: string;
  source_config?: any;
}

export interface TaskDAGRequest {
  task_type: 'bronze' | 'silver' | 'gold';
  task_label?: string;
  schedule?: string | null;
  start_date?: string;
  retries?: number;
  retry_delay_min?: number;
  owner?: string;
  source_type?: string;
  source_config?: any;
}

export interface MasterDAGRequest {
  dag_ids: string[];
  schedule?: string | null;
  start_date?: string;
  retries?: number;
  retry_delay_min?: number;
  owner?: string;
}

export interface DAGInfo {
  dag_id: string;
  dag_type: string;
  source_type?: string;
  filename: string;
  filepath: string;
  schedule?: string | null;
  child_dags?: string[];
}

export interface GenerateDAGResponse {
  project_id: string;
  project_name: string;
  dags: DAGInfo[];
  master: DAGInfo | null;
  message: string;
}

export interface DAGFileInfo {
  filename: string;
  filepath: string;
  size_bytes: number;
  modified_at: string;
}

export const dagApi = {
  /** Create a single task DAG (bronze/silver/gold) */
  createTaskDag: (projectId: string, req: TaskDAGRequest) =>
    api.post<DAGInfo>(`/api/dags/${projectId}/task-dag`, req).then(r => r.data),

  /** Create a master DAG that chains task DAGs in order */
  createMasterDag: (projectId: string, req: MasterDAGRequest) =>
    api.post<DAGInfo>(`/api/dags/${projectId}/master-dag`, req).then(r => r.data),

  /** Legacy: generate all DAGs at once */
  generate: (projectId: string, config: DAGScheduleConfig) =>
    api.post<GenerateDAGResponse>(`/api/dags/${projectId}/generate`, config).then(r => r.data),

  list: (projectId: string) =>
    api.get<DAGFileInfo[]>(`/api/dags/${projectId}`).then(r => r.data),

  deleteAll: (projectId: string) =>
    api.delete(`/api/dags/${projectId}`).then(r => r.data),

  deleteOne: (projectId: string, filename: string) =>
    api.delete(`/api/dags/${projectId}/${filename}`).then(r => r.data),
};

export default api;

// ============================================================================
// Silver Transformation API
// ============================================================================

export interface SilverTransformation {
  id: string;
  pipeline_id: string;
  name: string;
  description: string | null;
  status: string;
  generated_code: string | null;
  confirmed_code: string | null;
  input_schema: any[];
  sample_input: any[];
  sample_output: any[];
  conversation_count: number;
  task_order: number;
  created_at: string;
  updated_at: string;
}

export interface ConversationMessage {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  code_block: string | null;
  dry_run_result: any | null;
  message_order: number;
  created_at: string;
}

export interface ChatResponse {
  type: 'clarification' | 'code' | 'error';
  content: string;
  code: string | null;
  message_id: string;
  transformation_status: string;
}

export interface DryRunResult {
  success: boolean;
  output_rows: any[];
  output_schema: { name: string; type: string; nullable: boolean }[];
  row_count: number;
  error: string | null;
  validation_message: string;
}

export const silverApi = {
  /** Create a new Silver transformation */
  create: (projectId: string, data: { name: string; description?: string }) =>
    api.post<SilverTransformation>(`/api/silver/${projectId}/transformations`, data).then(r => r.data),

  /** List all transformations for a project */
  list: (projectId: string) =>
    api.get<SilverTransformation[]>(`/api/silver/${projectId}/transformations`).then(r => r.data),

  /** Get a specific transformation */
  get: (projectId: string, transformId: string) =>
    api.get<SilverTransformation>(`/api/silver/${projectId}/transformations/${transformId}`).then(r => r.data),

  /** Delete a transformation */
  delete: (projectId: string, transformId: string) =>
    api.delete(`/api/silver/${projectId}/transformations/${transformId}`).then(r => r.data),

  /** Get conversation messages */
  getMessages: (projectId: string, transformId: string) =>
    api.get<ConversationMessage[]>(`/api/silver/${projectId}/transformations/${transformId}/messages`).then(r => r.data),

  /** Send a chat message — returns AI response */
  chat: (projectId: string, transformId: string, message: string) =>
    api.post<ChatResponse>(`/api/silver/${projectId}/transformations/${transformId}/chat`, { message }).then(r => r.data),

  /** Clear chat history (start new conversation) */
  clearChat: (projectId: string, transformId: string) =>
    api.post(`/api/silver/${projectId}/transformations/${transformId}/clear-chat`).then(r => r.data),

  /** Update code (user edited in editor) */
  updateCode: (projectId: string, transformId: string, code: string) =>
    api.put(`/api/silver/${projectId}/transformations/${transformId}/code`, { code }).then(r => r.data),

  /** Dry-run the transformation on sample data */
  dryRun: (projectId: string, transformId: string) =>
    api.post<DryRunResult>(`/api/silver/${projectId}/transformations/${transformId}/dry-run`).then(r => r.data),

  /** Confirm the transformation code */
  confirm: (projectId: string, transformId: string, code: string) =>
    api.post<SilverTransformation>(`/api/silver/${projectId}/transformations/${transformId}/confirm`, { code }).then(r => r.data),
};
