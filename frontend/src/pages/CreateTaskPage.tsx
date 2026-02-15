import React, { useState, useCallback, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography, Box, Card, CardContent, TextField, Button,
  FormControl, InputLabel, Select, MenuItem, Alert, Grid,
  RadioGroup, FormControlLabel, Radio, FormLabel,
  LinearProgress, IconButton, List, ListItem, ListItemIcon,
  ListItemText, ListItemSecondaryAction, Paper, Chip,
  Tabs, Tab, Switch,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  InsertDriveFile as FileIcon,
  Delete as DeleteIcon,
  Api as ApiIcon,
  Stream as KafkaIcon,
} from '@mui/icons-material';
import { pipelineApi, uploadApi, SourceType } from '../services/api';

// ============================================================================
// Source mode tabs
// ============================================================================
type SourceMode = 'file' | 'api' | 'kafka';

const FILE_TYPES: { value: SourceType; label: string; desc: string }[] = [
  { value: 'csv',     label: 'CSV Files',    desc: 'Comma/delimiter separated value files' },
  { value: 'json',    label: 'JSON Files',   desc: 'JSON or JSON Lines files' },
  { value: 'parquet', label: 'Parquet Files', desc: 'Apache Parquet columnar files' },
];

const ACCEPTED_EXTENSIONS: Record<string, string[]> = {
  csv: ['.csv', '.tsv', '.txt'],
  json: ['.json', '.jsonl', '.ndjson'],
  parquet: ['.parquet', '.parq'],
};

export default function CreateTaskPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();

  // Source mode
  const [sourceMode, setSourceMode] = useState<SourceMode>('file');

  // ---------- FILE source state ----------
  const [sourceType, setSourceType] = useState<SourceType>('csv');
  const [csvDelimiter, setCsvDelimiter] = useState(',');
  const [csvHeader, setCsvHeader] = useState(true);
  const [csvEncoding, setCsvEncoding] = useState('utf-8');
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ---------- API source state ----------
  const [apiEndpoint, setApiEndpoint] = useState('');
  const [apiMethod, setApiMethod] = useState('GET');
  const [apiHeaders, setApiHeaders] = useState('{}');
  const [apiBody, setApiBody] = useState('{}');
  const [apiAuthType, setApiAuthType] = useState('none');
  const [apiUsername, setApiUsername] = useState('');
  const [apiPassword, setApiPassword] = useState('');
  const [apiBearerToken, setApiBearerToken] = useState('');
  const [apiDataKey, setApiDataKey] = useState('');
  const [apiPaginationEnabled, setApiPaginationEnabled] = useState(false);
  const [apiNextKey, setApiNextKey] = useState('');
  const [apiMaxPages, setApiMaxPages] = useState(100);

  // ---------- KAFKA source state ----------
  const [kafkaBootstrap, setKafkaBootstrap] = useState('');
  const [kafkaTopic, setKafkaTopic] = useState('');
  const [kafkaGroupId, setKafkaGroupId] = useState('');
  const [kafkaOffsetReset, setKafkaOffsetReset] = useState('earliest');
  const [kafkaMaxMessages, setKafkaMaxMessages] = useState(10000);
  const [kafkaTimeout, setKafkaTimeout] = useState(30000);

  // ---------- Common state ----------
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // =========================================================================
  // File handling
  // =========================================================================
  const isAcceptedFile = useCallback((file: File) => {
    const ext = '.' + file.name.split('.').pop()?.toLowerCase();
    return (ACCEPTED_EXTENSIONS[sourceType] || []).includes(ext);
  }, [sourceType]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation(); setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation(); setIsDragOver(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation(); setIsDragOver(false);
    const valid = Array.from(e.dataTransfer.files).filter(isAcceptedFile);
    if (valid.length === 0) {
      setError(`No valid ${sourceType.toUpperCase()} files. Accepted: ${ACCEPTED_EXTENSIONS[sourceType]?.join(', ')}`);
      return;
    }
    setSelectedFiles(prev => {
      const existing = new Set(prev.map(f => f.name));
      return [...prev, ...valid.filter(f => !existing.has(f.name))];
    });
    setError(null);
  }, [isAcceptedFile, sourceType]);

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files) return;
    const files = Array.from(e.target.files).filter(isAcceptedFile);
    setSelectedFiles(prev => {
      const existing = new Set(prev.map(f => f.name));
      return [...prev, ...files.filter(f => !existing.has(f.name))];
    });
    if (fileInputRef.current) fileInputRef.current.value = '';
  }, [isAcceptedFile]);

  const removeFile = useCallback((name: string) => {
    setSelectedFiles(prev => prev.filter(f => f.name !== name));
  }, []);

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  // =========================================================================
  // Submit handlers
  // =========================================================================
  const handleSubmitFile = async () => {
    if (!projectId) return;
    if (selectedFiles.length === 0) {
      setError('Please select at least one data file');
      return;
    }

    setLoading(true); setError(null); setUploading(true); setUploadProgress(20);

    try {
      await pipelineApi.update(projectId, { source_type: sourceType } as any);

      setUploadProgress(40);
      const uploadResult = await uploadApi.uploadFiles(projectId, selectedFiles);
      setUploadProgress(70);

      if (uploadResult.total_uploaded === 0) {
        setError('No files uploaded successfully');
        setLoading(false); setUploading(false);
        return;
      }

      setUploadProgress(85);
      await pipelineApi.configureSource(projectId, {
        source_type: sourceType,
        local_config: {
          file_path: uploadResult.upload_dir,
          file_format: uploadResult.detected_format || sourceType,
          csv_delimiter: csvDelimiter,
          csv_header: csvHeader,
          csv_encoding: csvEncoding,
        },
      });

      setUploadProgress(100); setUploading(false);
      navigate(`/project/${projectId}/task/bronze/schema`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create task');
    } finally {
      setLoading(false); setUploading(false);
    }
  };

  const handleSubmitApi = async () => {
    if (!projectId) return;
    if (!apiEndpoint.trim()) {
      setError('API endpoint URL is required');
      return;
    }

    setLoading(true); setError(null);
    try {
      let parsedHeaders = {};
      let parsedBody = {};
      try { parsedHeaders = JSON.parse(apiHeaders); } catch { /* keep empty */ }
      try { parsedBody = JSON.parse(apiBody); } catch { /* keep empty */ }

      const credentials: Record<string, string> = {};
      if (apiAuthType === 'basic') {
        credentials.username = apiUsername;
        credentials.password = apiPassword;
      } else if (apiAuthType === 'bearer') {
        credentials.token = apiBearerToken;
      }

      await pipelineApi.update(projectId, { source_type: 'api' } as any);

      await pipelineApi.configureSource(projectId, {
        source_type: 'api',
        api_config: {
          api_endpoint: apiEndpoint,
          api_method: apiMethod,
          api_headers: parsedHeaders,
          api_body: parsedBody,
          api_auth_type: apiAuthType,
          api_credentials: credentials,
          data_key: apiDataKey || null,
          pagination: apiPaginationEnabled
            ? { next_key: apiNextKey || null, max_pages: apiMaxPages }
            : {},
        },
      });

      navigate(`/project/${projectId}/task/bronze/schema`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure API source');
    } finally {
      setLoading(false);
    }
  };

  const handleSubmitKafka = async () => {
    if (!projectId) return;
    if (!kafkaBootstrap.trim() || !kafkaTopic.trim()) {
      setError('Kafka bootstrap servers and topic are required');
      return;
    }

    setLoading(true); setError(null);
    try {
      await pipelineApi.update(projectId, { source_type: 'kafka' } as any);

      await pipelineApi.configureSource(projectId, {
        source_type: 'kafka',
        kafka_config: {
          kafka_bootstrap: kafkaBootstrap,
          kafka_topic: kafkaTopic,
          kafka_group_id: kafkaGroupId || undefined,
          kafka_config: {
            auto_offset_reset: kafkaOffsetReset,
            max_messages: kafkaMaxMessages,
            consumer_timeout_ms: kafkaTimeout,
          },
        },
      });

      navigate(`/project/${projectId}`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to configure Kafka source');
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = () => {
    if (sourceMode === 'file') handleSubmitFile();
    else if (sourceMode === 'api') handleSubmitApi();
    else if (sourceMode === 'kafka') handleSubmitKafka();
  };

  const acceptedExts = ACCEPTED_EXTENSIONS[sourceType]?.join(',') || '';

  // =========================================================================
  // Render
  // =========================================================================
  return (
    <Box maxWidth={750} mx="auto">
      <Typography variant="h4" gutterBottom>Add Bronze Task</Typography>
      <Typography variant="body2" color="text.secondary" mb={3}>
        Configure how data enters your pipeline. Choose a source type below.
      </Typography>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {/* Source mode tabs */}
      <Card sx={{ mb: 3 }}>
        <Tabs
          value={sourceMode}
          onChange={(_, v) => setSourceMode(v)}
          variant="fullWidth"
          sx={{ borderBottom: 1, borderColor: 'divider' }}
        >
          <Tab icon={<UploadIcon />} label="File Upload" value="file" />
          <Tab icon={<ApiIcon />} label="REST API" value="api" />
          <Tab icon={<KafkaIcon />} label="Kafka" value="kafka" />
        </Tabs>
      </Card>

      {/* ================================================================== */}
      {/* FILE SOURCE */}
      {/* ================================================================== */}
      {sourceMode === 'file' && (
        <>
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <FormControl component="fieldset">
                <FormLabel sx={{ mb: 1.5 }}>File Format</FormLabel>
                <RadioGroup
                  row value={sourceType}
                  onChange={e => setSourceType(e.target.value as SourceType)}
                >
                  {FILE_TYPES.map(s => (
                    <FormControlLabel
                      key={s.value} value={s.value}
                      control={<Radio />} label={s.label}
                    />
                  ))}
                </RadioGroup>
              </FormControl>
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>Upload Files</Typography>
              <input
                type="file" ref={fileInputRef} style={{ display: 'none' }}
                multiple accept={acceptedExts} onChange={handleFileSelect}
              />
              <Paper
                variant="outlined"
                onDragOver={handleDragOver} onDragLeave={handleDragLeave} onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                sx={{
                  p: 4, mb: 2, textAlign: 'center', cursor: 'pointer',
                  borderStyle: 'dashed', borderWidth: 2,
                  borderColor: isDragOver ? 'primary.main' : 'rgba(255,255,255,0.15)',
                  bgcolor: isDragOver ? 'rgba(144,202,249,0.04)' : 'transparent',
                  transition: 'all 0.2s ease',
                  '&:hover': { borderColor: 'primary.main', bgcolor: 'rgba(144,202,249,0.04)' },
                }}
              >
                <UploadIcon sx={{ fontSize: 48, color: isDragOver ? 'primary.main' : 'text.secondary', mb: 1 }} />
                <Typography variant="body1" gutterBottom>
                  {isDragOver ? 'Drop files here!' : 'Drag & drop files here, or click to browse'}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Accepted: {ACCEPTED_EXTENSIONS[sourceType]?.join(', ')}
                </Typography>
              </Paper>

              {selectedFiles.length > 0 && (
                <Box>
                  <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
                    <Typography variant="subtitle2">
                      {selectedFiles.length} file{selectedFiles.length > 1 ? 's' : ''} selected
                    </Typography>
                    <Chip
                      label={`Total: ${formatSize(selectedFiles.reduce((s, f) => s + f.size, 0))}`}
                      size="small" color="primary" variant="outlined"
                    />
                  </Box>
                  <List dense sx={{ borderRadius: 1, border: '1px solid', borderColor: 'divider' }}>
                    {selectedFiles.map((file, idx) => (
                      <ListItem key={file.name} divider={idx < selectedFiles.length - 1}>
                        <ListItemIcon sx={{ minWidth: 36 }}><FileIcon color="primary" /></ListItemIcon>
                        <ListItemText primary={file.name} secondary={formatSize(file.size)} />
                        <ListItemSecondaryAction>
                          <IconButton edge="end" size="small" onClick={() => removeFile(file.name)}>
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </ListItemSecondaryAction>
                      </ListItem>
                    ))}
                  </List>
                </Box>
              )}

              {uploading && (
                <Box sx={{ mt: 2 }}>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Uploading & configuring...
                  </Typography>
                  <LinearProgress variant="determinate" value={uploadProgress} />
                </Box>
              )}
            </CardContent>
          </Card>

          {sourceType === 'csv' && (
            <Card sx={{ mb: 3 }}>
              <CardContent>
                <Typography variant="h6" gutterBottom>CSV Options</Typography>
                <Grid container spacing={2}>
                  <Grid item xs={4}>
                    <TextField fullWidth label="Delimiter" value={csvDelimiter}
                      onChange={e => setCsvDelimiter(e.target.value)} size="small" />
                  </Grid>
                  <Grid item xs={4}>
                    <FormControl fullWidth size="small">
                      <InputLabel>Header Row</InputLabel>
                      <Select value={csvHeader ? 'yes' : 'no'} label="Header Row"
                        onChange={e => setCsvHeader(e.target.value === 'yes')}>
                        <MenuItem value="yes">Yes</MenuItem>
                        <MenuItem value="no">No</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                  <Grid item xs={4}>
                    <TextField fullWidth label="Encoding" value={csvEncoding}
                      onChange={e => setCsvEncoding(e.target.value)} size="small" />
                  </Grid>
                </Grid>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* ================================================================== */}
      {/* API SOURCE */}
      {/* ================================================================== */}
      {sourceMode === 'api' && (
        <>
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>API Configuration</Typography>
              <Grid container spacing={2}>
                <Grid item xs={9}>
                  <TextField fullWidth label="Endpoint URL" placeholder="https://api.example.com/data"
                    value={apiEndpoint} onChange={e => setApiEndpoint(e.target.value)} size="small" />
                </Grid>
                <Grid item xs={3}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Method</InputLabel>
                    <Select value={apiMethod} label="Method"
                      onChange={e => setApiMethod(e.target.value)}>
                      <MenuItem value="GET">GET</MenuItem>
                      <MenuItem value="POST">POST</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <TextField fullWidth label="Response Data Key" placeholder="data, results, items (leave empty if root is array)"
                    value={apiDataKey} onChange={e => setApiDataKey(e.target.value)} size="small"
                    helperText="JSON key containing the array of records" />
                </Grid>
                <Grid item xs={12}>
                  <TextField fullWidth multiline rows={2} label="Headers (JSON)" value={apiHeaders}
                    onChange={e => setApiHeaders(e.target.value)} size="small"
                    placeholder='{"Content-Type": "application/json"}' />
                </Grid>
                {apiMethod === 'POST' && (
                  <Grid item xs={12}>
                    <TextField fullWidth multiline rows={2} label="Request Body (JSON)" value={apiBody}
                      onChange={e => setApiBody(e.target.value)} size="small" />
                  </Grid>
                )}
              </Grid>
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>Authentication</Typography>
              <FormControl fullWidth size="small" sx={{ mb: 2 }}>
                <InputLabel>Auth Type</InputLabel>
                <Select value={apiAuthType} label="Auth Type"
                  onChange={e => setApiAuthType(e.target.value)}>
                  <MenuItem value="none">None</MenuItem>
                  <MenuItem value="basic">Basic Auth</MenuItem>
                  <MenuItem value="bearer">Bearer Token</MenuItem>
                </Select>
              </FormControl>
              {apiAuthType === 'basic' && (
                <Grid container spacing={2}>
                  <Grid item xs={6}>
                    <TextField fullWidth label="Username" value={apiUsername}
                      onChange={e => setApiUsername(e.target.value)} size="small" />
                  </Grid>
                  <Grid item xs={6}>
                    <TextField fullWidth label="Password" type="password" value={apiPassword}
                      onChange={e => setApiPassword(e.target.value)} size="small" />
                  </Grid>
                </Grid>
              )}
              {apiAuthType === 'bearer' && (
                <TextField fullWidth label="Bearer Token" value={apiBearerToken}
                  onChange={e => setApiBearerToken(e.target.value)} size="small" />
              )}
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography variant="h6">Pagination</Typography>
                <FormControlLabel
                  control={<Switch checked={apiPaginationEnabled}
                    onChange={e => setApiPaginationEnabled(e.target.checked)} />}
                  label="Enable"
                />
              </Box>
              {apiPaginationEnabled && (
                <Grid container spacing={2} sx={{ mt: 0.5 }}>
                  <Grid item xs={8}>
                    <TextField fullWidth label="Next Page Key" placeholder="next, next_url, links.next"
                      value={apiNextKey} onChange={e => setApiNextKey(e.target.value)} size="small"
                      helperText="JSON key in response containing next page URL" />
                  </Grid>
                  <Grid item xs={4}>
                    <TextField fullWidth label="Max Pages" type="number" value={apiMaxPages}
                      onChange={e => setApiMaxPages(parseInt(e.target.value) || 100)} size="small" />
                  </Grid>
                </Grid>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {/* ================================================================== */}
      {/* KAFKA SOURCE */}
      {/* ================================================================== */}
      {sourceMode === 'kafka' && (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>Kafka Configuration</Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField fullWidth label="Bootstrap Servers"
                  placeholder="kafka:9092, kafka-2:9092"
                  value={kafkaBootstrap} onChange={e => setKafkaBootstrap(e.target.value)} size="small"
                  helperText="Comma-separated list of Kafka broker addresses" />
              </Grid>
              <Grid item xs={6}>
                <TextField fullWidth label="Topic" placeholder="my-topic"
                  value={kafkaTopic} onChange={e => setKafkaTopic(e.target.value)} size="small" />
              </Grid>
              <Grid item xs={6}>
                <TextField fullWidth label="Consumer Group ID" placeholder="auto-generated if empty"
                  value={kafkaGroupId} onChange={e => setKafkaGroupId(e.target.value)} size="small" />
              </Grid>
              <Grid item xs={4}>
                <FormControl fullWidth size="small">
                  <InputLabel>Offset Reset</InputLabel>
                  <Select value={kafkaOffsetReset} label="Offset Reset"
                    onChange={e => setKafkaOffsetReset(e.target.value)}>
                    <MenuItem value="earliest">Earliest</MenuItem>
                    <MenuItem value="latest">Latest</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={4}>
                <TextField fullWidth label="Max Messages" type="number" value={kafkaMaxMessages}
                  onChange={e => setKafkaMaxMessages(parseInt(e.target.value) || 10000)} size="small"
                  helperText="Per DAG run" />
              </Grid>
              <Grid item xs={4}>
                <TextField fullWidth label="Timeout (ms)" type="number" value={kafkaTimeout}
                  onChange={e => setKafkaTimeout(parseInt(e.target.value) || 30000)} size="small"
                  helperText="Consumer timeout" />
              </Grid>
            </Grid>
          </CardContent>
        </Card>
      )}

      {/* Actions */}
      <Box display="flex" justifyContent="space-between">
        <Button onClick={() => navigate(`/project/${projectId}`)}>Cancel</Button>
        <Button
          variant="contained"
          onClick={handleSubmit}
          disabled={loading ||
            (sourceMode === 'file' && selectedFiles.length === 0) ||
            (sourceMode === 'api' && !apiEndpoint.trim()) ||
            (sourceMode === 'kafka' && (!kafkaBootstrap.trim() || !kafkaTopic.trim()))
          }
          size="large"
        >
          {loading
            ? (uploading ? 'Uploading...' : 'Configuring...')
            : sourceMode === 'file'
              ? 'Upload & Detect Schema'
              : 'Save & Configure Source'
          }
        </Button>
      </Box>
    </Box>
  );
}
