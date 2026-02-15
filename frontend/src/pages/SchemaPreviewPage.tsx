import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography, Box, Card, CardContent, Button, Alert,
  CircularProgress, Chip, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Paper, Select,
  MenuItem, FormControl, Accordion, AccordionSummary,
  AccordionDetails, LinearProgress, TextField, IconButton,
  Tooltip, Divider, Switch, FormControlLabel,
} from '@mui/material';
import {
  ExpandMore as ExpandIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Edit as EditIcon,
  Save as SaveIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import {
  schemaApi, bronzeApi, pipelineApi,
  SchemaDetectionResult, FieldSchema, FileInfo, BronzeIngestionStatus,
} from '../services/api';

const TYPE_OPTIONS = ['string', 'integer', 'float', 'boolean', 'timestamp', 'date', 'long', 'double'];

function confidenceColor(c: number): 'success' | 'warning' | 'error' {
  if (c >= 0.9) return 'success';
  if (c >= 0.7) return 'warning';
  return 'error';
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function SchemaPreviewPage() {
  const { projectId: pipelineId } = useParams<{ projectId: string; taskId: string }>();
  const navigate = useNavigate();

  const [loading, setLoading] = useState(true);
  const [detecting, setDetecting] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  // Schema data
  const [schema, setSchema] = useState<SchemaDetectionResult | null>(null);
  const [schemaId, setSchemaId] = useState<string | null>(null);

  // User edits
  const [editedFields, setEditedFields] = useState<Record<string, Partial<FieldSchema & { new_type?: string; new_name?: string; excluded?: boolean }>>>({});
  const [excludedFiles, setExcludedFiles] = useState<Set<string>>(new Set());
  const [includedFiles, setIncludedFiles] = useState<Set<string>>(new Set());

  // Ingestion tracking
  const [ingestionStatus, setIngestionStatus] = useState<BronzeIngestionStatus | null>(null);
  const [ingestionPolling, setIngestionPolling] = useState(false);

  // Source type awareness
  const [sourceType, setSourceType] = useState<string>('csv');

  // =========================================================================
  // Load / detect schema
  // =========================================================================

  const detectSchema = async () => {
    if (!pipelineId) return;
    setDetecting(true);
    setError(null);

    try {
      // Fetch pipeline info to determine source type
      try {
        const pipeline = await pipelineApi.get(pipelineId);
        if (pipeline.source_type) setSourceType(pipeline.source_type);
      } catch { /* keep default */ }

      const result = await schemaApi.detect(pipelineId);
      setSchema(result);

      // Use schema_id directly from the detect response
      if (result.schema_id) {
        setSchemaId(result.schema_id);
      } else {
        // Fallback: fetch from versions list
        const versions = await schemaApi.listVersions(pipelineId);
        if (versions.length > 0) {
          setSchemaId(versions[0].id);
        }
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Schema detection failed');
    } finally {
      setDetecting(false);
      setLoading(false);
    }
  };

  useEffect(() => {
    detectSchema();
    // eslint-disable-next-line
  }, [pipelineId]);

  // =========================================================================
  // User edit handlers
  // =========================================================================

  const handleTypeChange = (fieldName: string, newType: string) => {
    setEditedFields(prev => ({
      ...prev,
      [fieldName]: { ...prev[fieldName], new_type: newType },
    }));
  };

  const handleRename = (fieldName: string, newName: string) => {
    setEditedFields(prev => ({
      ...prev,
      [fieldName]: { ...prev[fieldName], new_name: newName },
    }));
  };

  const handleNullableToggle = (fieldName: string, nullable: boolean) => {
    setEditedFields(prev => ({
      ...prev,
      [fieldName]: { ...prev[fieldName], nullable },
    }));
  };

  const toggleFileExclude = (path: string) => {
    setExcludedFiles(prev => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  const toggleFileInclude = (path: string) => {
    setIncludedFiles(prev => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  // =========================================================================
  // Confirm schema → Bronze ingestion
  // =========================================================================

  const handleConfirm = async () => {
    if (!pipelineId || !schemaId) return;
    setConfirming(true);
    setError(null);

    try {
      // Build field overrides
      const fieldOverrides = Object.entries(editedFields).map(([name, edits]) => ({
        name,
        new_type: edits.new_type || undefined,
        new_name: edits.new_name || undefined,
        nullable: edits.nullable,
        exclude: edits.excluded || false,
      }));

      const response = await schemaApi.confirm({
        pipeline_id: pipelineId,
        schema_id: schemaId,
        field_overrides: fieldOverrides,
        exclude_files: Array.from(excludedFiles),
        include_files: Array.from(includedFiles),
      });

      setSuccessMsg(response.message);

      // Start polling for ingestion status
      // We'll poll the pipeline's ingestions list
      setIngestionPolling(true);
      pollIngestionStatus();

    } catch (err: any) {
      setError(err.response?.data?.detail || 'Schema confirmation failed');
    } finally {
      setConfirming(false);
    }
  };

  const pollIngestionStatus = async () => {
    if (!pipelineId) return;

    const poll = async () => {
      try {
        const ingestions = await bronzeApi.listIngestions(pipelineId);
        if (ingestions.length > 0) {
          const latest = ingestions[0];
          const status = await bronzeApi.getIngestionStatus(latest.id);
          setIngestionStatus(status);

          if (status.status === 'running') {
            setTimeout(poll, 3000); // Poll every 3 seconds
          } else {
            setIngestionPolling(false);
          }
        }
      } catch (err) {
        console.error('Polling failed:', err);
        setIngestionPolling(false);
      }
    };

    setTimeout(poll, 2000); // Initial delay
  };

  // =========================================================================
  // Render
  // =========================================================================

  if (loading || detecting) {
    return (
      <Box display="flex" flexDirection="column" alignItems="center" mt={10}>
        <CircularProgress />
        <Typography mt={2}>
          {detecting ? (sourceType === 'api' ? 'Fetching API & detecting schema...' : 'Scanning files & detecting schema...') : 'Loading...'}
        </Typography>
      </Box>
    );
  }

  if (!schema) {
    return (
      <Box mt={4}>
        <Alert severity="error">{error || 'No schema data available'}</Alert>
        <Button sx={{ mt: 2 }} onClick={() => navigate(`/project/${pipelineId}`)}>Back to Project</Button>
      </Box>
    );
  }

  const hasEdits = Object.keys(editedFields).length > 0 || excludedFiles.size > 0 || includedFiles.size > 0;

  return (
    <Box maxWidth={1100} mx="auto">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">Schema Preview</Typography>
        <Box>
          <Chip
            label={`Confidence: ${(schema.detection_confidence * 100).toFixed(1)}%`}
            color={confidenceColor(schema.detection_confidence)}
            sx={{ mr: 1 }}
          />
          <Chip
            label={sourceType === 'api' ? `API source · ${schema.sample_row_count} records` : `${schema.total_files} file(s) found`}
            variant="outlined"
          />
        </Box>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {successMsg && <Alert severity="success" sx={{ mb: 2 }}>{successMsg}</Alert>}

      {/* Ingestion Status */}
      {ingestionStatus && (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Bronze Ingestion Status
            </Typography>
            <Box display="flex" alignItems="center" gap={2} mb={1}>
              <Chip
                label={ingestionStatus.status}
                color={
                  ingestionStatus.status === 'success' ? 'success' :
                  ingestionStatus.status === 'failed' ? 'error' : 'info'
                }
              />
              <Typography variant="body2">
                Records: {ingestionStatus.total_records.toLocaleString()} |
                Files: {ingestionStatus.files_processed} processed, {ingestionStatus.files_skipped} skipped |
                Duration: {ingestionStatus.duration_seconds.toFixed(1)}s
              </Typography>
            </Box>
            {ingestionStatus.status === 'running' && <LinearProgress />}
            {ingestionStatus.error_message && (
              <Alert severity="error" sx={{ mt: 1 }}>{ingestionStatus.error_message}</Alert>
            )}
          </CardContent>
        </Card>
      )}

      {/* Fields Table */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Detected Fields ({schema.fields.length})
          </Typography>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            Review the detected schema. You can change types, rename fields, or toggle nullable.
          </Typography>

          <TableContainer component={Paper} variant="outlined" sx={{ mt: 2 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell><strong>Field Name</strong></TableCell>
                  <TableCell><strong>Detected Type</strong></TableCell>
                  <TableCell><strong>Nullable</strong></TableCell>
                  <TableCell><strong>Confidence</strong></TableCell>
                  <TableCell><strong>Unique</strong></TableCell>
                  <TableCell><strong>Nulls</strong></TableCell>
                  <TableCell><strong>Sample Values</strong></TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {schema.fields.map((field) => {
                  const edits = editedFields[field.name] || {};
                  return (
                    <TableRow key={field.name} hover>
                      <TableCell>
                        <TextField
                          variant="standard"
                          size="small"
                          value={edits.new_name ?? field.name}
                          onChange={e => handleRename(field.name, e.target.value)}
                          sx={{ minWidth: 120 }}
                        />
                      </TableCell>
                      <TableCell>
                        <FormControl size="small" variant="standard" sx={{ minWidth: 100 }}>
                          <Select
                            value={edits.new_type ?? field.detected_type}
                            onChange={e => handleTypeChange(field.name, e.target.value)}
                          >
                            {TYPE_OPTIONS.map(t => (
                              <MenuItem key={t} value={t}>{t}</MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                      </TableCell>
                      <TableCell>
                        <Switch
                          size="small"
                          checked={edits.nullable ?? field.nullable}
                          onChange={e => handleNullableToggle(field.name, e.target.checked)}
                        />
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={`${(field.confidence * 100).toFixed(0)}%`}
                          size="small"
                          color={confidenceColor(field.confidence)}
                        />
                      </TableCell>
                      <TableCell>{field.unique_count}</TableCell>
                      <TableCell>{field.null_count}</TableCell>
                      <TableCell>
                        <Typography variant="caption" sx={{ fontFamily: 'monospace' }}>
                          {field.sample_values.slice(0, 3).map(String).join(', ')}
                        </Typography>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>

      {/* Compatible Files */}
      <Accordion defaultExpanded={true} sx={{ mb: 2 }}>
        <AccordionSummary expandIcon={<ExpandIcon />}>
          <CheckIcon color="success" sx={{ mr: 1 }} />
          <Typography variant="h6">
            {sourceType === 'api' ? 'Data Source' : `Compatible Files (${schema.compatible_files.length})`}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          {schema.compatible_files.length === 0 ? (
            <Typography color="text.secondary">No compatible files found.</Typography>
          ) : (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Include</TableCell>
                  <TableCell>Filename</TableCell>
                  <TableCell>Size</TableCell>
                  <TableCell>Rows</TableCell>
                  <TableCell>Path</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {schema.compatible_files.map((file) => (
                  <TableRow key={file.path} hover>
                    <TableCell>
                      <Switch
                        size="small"
                        checked={!excludedFiles.has(file.path)}
                        onChange={() => toggleFileExclude(file.path)}
                      />
                    </TableCell>
                    <TableCell>{file.filename}</TableCell>
                    <TableCell>{formatBytes(file.size_bytes)}</TableCell>
                    <TableCell>{file.row_count?.toLocaleString() ?? '—'}</TableCell>
                    <TableCell>
                      <Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'monospace' }}>
                        {file.path}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Incompatible Files */}
      {schema.incompatible_files.length > 0 && (
        <Accordion sx={{ mb: 3 }}>
          <AccordionSummary expandIcon={<ExpandIcon />}>
            <WarningIcon color="warning" sx={{ mr: 1 }} />
            <Typography variant="h6">
              Incompatible Files ({schema.incompatible_files.length})
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Force Include</TableCell>
                  <TableCell>Filename</TableCell>
                  <TableCell>Size</TableCell>
                  <TableCell>Mismatch Reason</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {schema.incompatible_files.map((file) => (
                  <TableRow key={file.path} hover>
                    <TableCell>
                      <Switch
                        size="small"
                        checked={includedFiles.has(file.path)}
                        onChange={() => toggleFileInclude(file.path)}
                      />
                    </TableCell>
                    <TableCell>{file.filename}</TableCell>
                    <TableCell>{formatBytes(file.size_bytes)}</TableCell>
                    <TableCell>
                      <Typography variant="body2" color="error">
                        {file.mismatch_reason}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </AccordionDetails>
        </Accordion>
      )}

      {/* Action Buttons */}
      <Box display="flex" justifyContent="space-between" mt={3} mb={5}>
        <Button onClick={() => navigate(`/project/${pipelineId}`)}>Back to Project</Button>
        <Box>
          <Button
            sx={{ mr: 2 }}
            startIcon={<RefreshIcon />}
            onClick={detectSchema}
            disabled={detecting}
          >
            Re-detect
          </Button>
          <Button
            variant="contained"
            color="primary"
            size="large"
            onClick={handleConfirm}
            disabled={confirming || ingestionPolling || schema.compatible_files.length === 0}
          >
            {confirming ? 'Confirming...' : sourceType === 'api' ? 'Confirm Schema' : 'Confirm Schema & Ingest to Bronze'}
          </Button>
        </Box>
      </Box>
    </Box>
  );
}
