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

  /** Test Kafka broker connectivity (requires source already saved) */
  testKafkaConnection: (pipelineId: string) =>
    api.post(`/api/pipelines/${pipelineId}/kafka/test-connection`).then(r => r.data),

  /** List available Kafka topics (requires source already saved) */
  listKafkaTopics: (pipelineId: string) =>
    api.get(`/api/pipelines/${pipelineId}/kafka/topics`).then(r => r.data),

  /** Test Kafka connection directly with bootstrap servers (before source is saved) */
  testKafkaConnectionDirect: (bootstrapServers: string) =>
    api.post(`/api/pipelines/kafka/test-connection-direct?bootstrap_servers=${encodeURIComponent(bootstrapServers)}`).then(r => r.data),
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

export interface DataPreview {
  total_records: number;
  columns: { name: string; type: string; nullable: boolean }[];
  rows: Record<string, any>[];
  preview_count: number;
  bronze_path?: string;
  silver_path?: string;
}

export const bronzeApi = {
  listIngestions: (pipelineId: string) =>
    api.get(`/api/bronze/${pipelineId}/ingestions`).then(r => r.data),

  getIngestionStatus: (ingestionId: string) =>
    api.get<BronzeIngestionStatus>(`/api/bronze/ingestion/${ingestionId}/status`).then(r => r.data),

  /** Preview sample rows from Bronze data */
  preview: (pipelineId: string, limit: number = 50) =>
    api.get<DataPreview>(`/api/bronze/${pipelineId}/preview`, { params: { limit } }).then(r => r.data),
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
  push_to_postgres?: boolean;
  postgres_table_name?: string;
  postgres_if_exists?: string;
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
  version: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface TransformResult {
  id: string;
  name: string;
  version: number;
  status: 'success' | 'failed';
  error?: string;
  duration_seconds: number;
}

export interface UploadToSilverResult {
  success: boolean;
  execution_id: string;
  input_records: number;
  output_records: number;
  output_path: string;
  duration_seconds: number;
  transformations_applied: number;
  error: string | null;
  transform_results: TransformResult[];
}

export interface SilverExecution {
  id: string;
  status: string;
  input_records: number;
  output_records: number;
  output_path: string;
  duration_seconds: number;
  transformations_applied: number;
  error: string | null;
  started_at: string;
  completed_at: string;
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

  /** Confirm the transformation code with a name */
  confirm: (projectId: string, transformId: string, name: string, code: string) =>
    api.post<SilverTransformation>(
      `/api/silver/${projectId}/transformations/${transformId}/confirm`,
      { name, code }
    ).then(r => r.data),

  /** Get version history for a transformation at a given task_order */
  getVersions: (projectId: string, taskOrder: number) =>
    api.get<SilverTransformation[]>(
      `/api/silver/${projectId}/transformations/order/${taskOrder}/versions`
    ).then(r => r.data),

  /** Upload to silver — apply all confirmed transforms to bronze data */
  uploadToSilver: (projectId: string, transformationIds?: string[]) =>
    api.post<UploadToSilverResult>(
      `/api/silver/${projectId}/upload-to-silver`,
      { transformation_ids: transformationIds || null }
    ).then(r => r.data),

  /** List silver execution history */
  listExecutions: (projectId: string) =>
    api.get<SilverExecution[]>(`/api/silver/${projectId}/executions`).then(r => r.data),

  /** Preview sample rows from Silver data */
  preview: (projectId: string, limit: number = 50) =>
    api.get<DataPreview>(`/api/silver/${projectId}/preview`, { params: { limit } }).then(r => r.data),

  /** Rollback to a specific version */
  rollback: (projectId: string, transformId: string) =>
    api.post<SilverTransformation>(`/api/silver/${projectId}/transformations/${transformId}/rollback`).then(r => r.data),

  /** Reorder transformations */
  reorder: (projectId: string, order: string[]) =>
    api.put(`/api/silver/${projectId}/transformations/reorder`, order).then(r => r.data),
};

// ============================================================================
// Gold Transformation API
// ============================================================================

export interface GoldTransformation {
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
  version: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface UploadToGoldResult {
  success: boolean;
  execution_id: string;
  input_records: number;
  output_records: number;
  output_path: string;
  duration_seconds: number;
  transformations_applied: number;
  error: string | null;
  transform_results: TransformResult[];
}

export interface GoldExecution {
  id: string;
  status: string;
  input_records: number;
  output_records: number;
  output_path: string;
  duration_seconds: number;
  transformations_applied: number;
  error: string | null;
  started_at: string;
  completed_at: string;
}

export interface PushToPostgresResult {
  success: boolean;
  push_id: string;
  table_name: string;
  records_pushed: number;
  duration_seconds: number;
  error: string | null;
}

export interface PostgresPushRecord {
  id: string;
  table_name: string;
  status: string;
  records_pushed: number;
  duration_seconds: number;
  error: string | null;
  created_at: string;
  completed_at: string;
}

export const goldApi = {
  /** Refresh Gold schema from latest Silver output */
  refreshSchema: (projectId: string) =>
    api.post<{ updated: number; columns: string[]; sample_rows: number }>(
      `/api/gold/${projectId}/refresh-schema`
    ).then(r => r.data),

  /** Create a new Gold transformation */
  create: (projectId: string, data: { name: string; description?: string }) =>
    api.post<GoldTransformation>(`/api/gold/${projectId}/transformations`, data).then(r => r.data),

  /** List all Gold transformations for a project */
  list: (projectId: string) =>
    api.get<GoldTransformation[]>(`/api/gold/${projectId}/transformations`).then(r => r.data),

  /** Get a specific Gold transformation */
  get: (projectId: string, transformId: string) =>
    api.get<GoldTransformation>(`/api/gold/${projectId}/transformations/${transformId}`).then(r => r.data),

  /** Delete a Gold transformation */
  delete: (projectId: string, transformId: string) =>
    api.delete(`/api/gold/${projectId}/transformations/${transformId}`).then(r => r.data),

  /** Get conversation messages */
  getMessages: (projectId: string, transformId: string) =>
    api.get<ConversationMessage[]>(`/api/gold/${projectId}/transformations/${transformId}/messages`).then(r => r.data),

  /** Send a chat message */
  chat: (projectId: string, transformId: string, message: string) =>
    api.post<ChatResponse>(`/api/gold/${projectId}/transformations/${transformId}/chat`, { message }).then(r => r.data),

  /** Clear chat history */
  clearChat: (projectId: string, transformId: string) =>
    api.post(`/api/gold/${projectId}/transformations/${transformId}/clear-chat`).then(r => r.data),

  /** Update code */
  updateCode: (projectId: string, transformId: string, code: string) =>
    api.put(`/api/gold/${projectId}/transformations/${transformId}/code`, { code }).then(r => r.data),

  /** Dry-run */
  dryRun: (projectId: string, transformId: string) =>
    api.post<DryRunResult>(`/api/gold/${projectId}/transformations/${transformId}/dry-run`).then(r => r.data),

  /** Confirm */
  confirm: (projectId: string, transformId: string, name: string, code: string) =>
    api.post<GoldTransformation>(
      `/api/gold/${projectId}/transformations/${transformId}/confirm`,
      { name, code }
    ).then(r => r.data),

  /** Get version history */
  getVersions: (projectId: string, taskOrder: number) =>
    api.get<GoldTransformation[]>(
      `/api/gold/${projectId}/transformations/order/${taskOrder}/versions`
    ).then(r => r.data),

  /** Upload to Gold */
  uploadToGold: (projectId: string, transformationIds?: string[]) =>
    api.post<UploadToGoldResult>(
      `/api/gold/${projectId}/upload-to-gold`,
      { transformation_ids: transformationIds || null }
    ).then(r => r.data),

  /** Push Gold data to Postgres */
  pushToPostgres: (projectId: string, tableName: string, ifExists: string = 'replace') =>
    api.post<PushToPostgresResult>(
      `/api/gold/${projectId}/push-to-postgres`,
      { table_name: tableName, if_exists: ifExists }
    ).then(r => r.data),

  /** List Gold execution history */
  listExecutions: (projectId: string) =>
    api.get<GoldExecution[]>(`/api/gold/${projectId}/executions`).then(r => r.data),

  /** List Postgres push history */
  listPostgresPushes: (projectId: string) =>
    api.get<PostgresPushRecord[]>(`/api/gold/${projectId}/postgres-pushes`).then(r => r.data),

  /** Preview Gold data */
  preview: (projectId: string, limit: number = 50) =>
    api.get<DataPreview>(`/api/gold/${projectId}/preview`, { params: { limit } }).then(r => r.data),

  /** Rollback to a specific version */
  rollback: (projectId: string, transformId: string) =>
    api.post<GoldTransformation>(`/api/gold/${projectId}/transformations/${transformId}/rollback`).then(r => r.data),

  /** Reorder Gold transformations */
  reorder: (projectId: string, order: string[]) =>
    api.put(`/api/gold/${projectId}/transformations/reorder`, order).then(r => r.data),
};
