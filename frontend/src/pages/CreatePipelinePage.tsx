import React, { useState, useCallback, useRef } from 'react';
import {
  Typography, Box, Stepper, Step, StepLabel, Card, CardContent,
  TextField, Button, FormControl, InputLabel, Select, MenuItem,
  Alert, Grid, RadioGroup, FormControlLabel, Radio, FormLabel,
  LinearProgress, IconButton, List, ListItem, ListItemIcon,
  ListItemText, ListItemSecondaryAction, Paper, Chip,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  InsertDriveFile as FileIcon,
  Delete as DeleteIcon,
  CheckCircle as CheckIcon,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { pipelineApi, uploadApi, SourceType } from '../services/api';

const SOURCE_TYPES: { value: SourceType; label: string; description: string; category: string }[] = [
  { value: 'csv',      label: 'CSV Files',     description: 'Comma/delimiter separated value files', category: 'local' },
  { value: 'json',     label: 'JSON Files',    description: 'JSON or JSON Lines files',              category: 'local' },
  { value: 'parquet',  label: 'Parquet Files',  description: 'Apache Parquet columnar files',         category: 'local' },
  { value: 'kafka',    label: 'Kafka Stream',   description: 'Real-time stream from Kafka topic',     category: 'stream' },
  { value: 'api',      label: 'REST API',       description: 'Ingest data from a REST API endpoint',  category: 'api' },
  { value: 'database', label: 'Database',       description: 'Connect to PostgreSQL, MySQL, etc.',    category: 'database' },
];

const STEPS = ['Pipeline Info', 'Select Source Type', 'Configure Source'];

export default function CreatePipelinePage() {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Step 1: Pipeline info
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');

  // Step 2: Source type
  const [sourceType, setSourceType] = useState<SourceType>('csv');

  // Step 3: Source config (varies by type)
  const [csvDelimiter, setCsvDelimiter] = useState(',');
  const [csvHeader, setCsvHeader] = useState(true);
  const [csvEncoding, setCsvEncoding] = useState('utf-8');

  // File upload state
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadedDir, setUploadedDir] = useState<string | null>(null);
  const [detectedFormat, setDetectedFormat] = useState<string | null>(null);
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // API source
  const [apiEndpoint, setApiEndpoint] = useState('');
  const [apiMethod, setApiMethod] = useState('GET');

  // Kafka source
  const [kafkaBootstrap, setKafkaBootstrap] = useState('');
  const [kafkaTopic, setKafkaTopic] = useState('');

  // Database source
  const [dbConnection, setDbConnection] = useState('');
  const [dbTable, setDbTable] = useState('');

  // =========================================================================
  // File drag-and-drop handlers
  // =========================================================================

  const ACCEPTED_EXTENSIONS: Record<string, string[]> = {
    csv: ['.csv', '.tsv', '.txt'],
    json: ['.json', '.jsonl', '.ndjson'],
    parquet: ['.parquet', '.parq'],
  };

  const isAcceptedFile = useCallback((file: File) => {
    const ext = '.' + file.name.split('.').pop()?.toLowerCase();
    const accepted = ACCEPTED_EXTENSIONS[sourceType] || [];
    return accepted.includes(ext);
  }, [sourceType]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
    const droppedFiles = Array.from(e.dataTransfer.files);
    const validFiles = droppedFiles.filter(isAcceptedFile);
    if (validFiles.length === 0) {
      setError(`No valid ${sourceType.toUpperCase()} files found. Accepted: ${ACCEPTED_EXTENSIONS[sourceType]?.join(', ')}`);
      return;
    }
    setSelectedFiles(prev => {
      const existing = new Set(prev.map(f => f.name));
      const newFiles = validFiles.filter(f => !existing.has(f.name));
      return [...prev, ...newFiles];
    });
    setError(null);
  }, [isAcceptedFile, sourceType]);

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files) return;
    const files = Array.from(e.target.files).filter(isAcceptedFile);
    setSelectedFiles(prev => {
      const existing = new Set(prev.map(f => f.name));
      const newFiles = files.filter(f => !existing.has(f.name));
      return [...prev, ...newFiles];
    });
    // Reset file input so same file can be re-selected
    if (fileInputRef.current) fileInputRef.current.value = '';
  }, [isAcceptedFile]);

  const removeFile = useCallback((fileName: string) => {
    setSelectedFiles(prev => prev.filter(f => f.name !== fileName));
  }, []);

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  // =========================================================================
  // Navigation & submit
  // =========================================================================

  const handleNext = () => {
    setError(null);
    if (activeStep === 0 && !name.trim()) {
      setError('Pipeline name is required');
      return;
    }
    if (activeStep === 2) {
      handleSubmit();
      return;
    }
    setActiveStep(prev => prev + 1);
  };

  const handleBack = () => setActiveStep(prev => prev - 1);

  const handleSubmit = async () => {
    setLoading(true);
    setError(null);

    try {
      // Step 1: Create pipeline
      const pipeline = await pipelineApi.create({
        name,
        description: description || undefined,
        source_type: sourceType,
      });

      // Step 2: For local file sources — upload files then configure
      if (['csv', 'json', 'parquet'].includes(sourceType)) {
        if (selectedFiles.length === 0) {
          setError('Please upload at least one data file');
          setLoading(false);
          return;
        }

        // Upload files
        setUploading(true);
        setUploadProgress(30);
        const uploadResult = await uploadApi.uploadFiles(pipeline.id, selectedFiles);
        setUploadProgress(70);

        if (uploadResult.total_errors > 0) {
          const errMsgs = uploadResult.errors.map((e: any) => `${e.filename}: ${e.error}`).join('; ');
          setError(`Some files failed to upload: ${errMsgs}`);
        }

        if (uploadResult.total_uploaded === 0) {
          setError('No files were uploaded successfully');
          setLoading(false);
          setUploading(false);
          return;
        }

        setUploadedDir(uploadResult.upload_dir);
        setDetectedFormat(uploadResult.detected_format);

        // Configure source with the container-internal path
        const sourceConfig: any = {
          source_type: sourceType,
          local_config: {
            file_path: uploadResult.upload_dir,
            file_format: uploadResult.detected_format || sourceType,
            csv_delimiter: csvDelimiter,
            csv_header: csvHeader,
            csv_encoding: csvEncoding,
          },
        };

        setUploadProgress(90);
        await pipelineApi.configureSource(pipeline.id, sourceConfig);
        setUploadProgress(100);
        setUploading(false);

      } else if (sourceType === 'api') {
        if (!apiEndpoint.trim()) {
          setError('API endpoint is required');
          setLoading(false);
          return;
        }
        await pipelineApi.configureSource(pipeline.id, {
          source_type: sourceType,
          api_config: { api_endpoint: apiEndpoint, api_method: apiMethod },
        });

      } else if (sourceType === 'kafka') {
        if (!kafkaBootstrap.trim() || !kafkaTopic.trim()) {
          setError('Kafka bootstrap server and topic are required');
          setLoading(false);
          return;
        }
        await pipelineApi.configureSource(pipeline.id, {
          source_type: sourceType,
          kafka_config: { kafka_bootstrap: kafkaBootstrap, kafka_topic: kafkaTopic },
        });

      } else if (sourceType === 'database') {
        if (!dbConnection.trim()) {
          setError('Database connection string is required');
          setLoading(false);
          return;
        }
        await pipelineApi.configureSource(pipeline.id, {
          source_type: sourceType,
          database_config: { db_connection: dbConnection, db_table: dbTable || undefined },
        });
      }

      // Navigate to schema detection
      navigate(`/pipeline/${pipeline.id}/schema`);

    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create pipeline');
    } finally {
      setLoading(false);
      setUploading(false);
    }
  };

  // =========================================================================
  // Render step content
  // =========================================================================

  const renderStep1 = () => (
    <Box>
      <Typography variant="h6" gutterBottom>Pipeline Information</Typography>
      <TextField
        fullWidth label="Pipeline Name" value={name}
        onChange={e => setName(e.target.value)}
        required sx={{ mb: 2 }}
        placeholder="e.g., Sales Data Pipeline"
      />
      <TextField
        fullWidth label="Description (optional)" value={description}
        onChange={e => setDescription(e.target.value)}
        multiline rows={3} sx={{ mb: 2 }}
        placeholder="Describe what this pipeline processes..."
      />
    </Box>
  );

  const renderStep2 = () => (
    <Box>
      <Typography variant="h6" gutterBottom>Select Data Source Type</Typography>

      <FormControl component="fieldset">
        <FormLabel sx={{ mb: 2 }}>What type of data source?</FormLabel>
        <RadioGroup value={sourceType} onChange={e => setSourceType(e.target.value as SourceType)}>
          <Typography variant="subtitle2" color="text.secondary" sx={{ mt: 1, mb: 0.5 }}>
            📁 Local Files
          </Typography>
          {SOURCE_TYPES.filter(s => s.category === 'local').map(s => (
            <FormControlLabel
              key={s.value} value={s.value}
              control={<Radio />}
              label={
                <Box>
                  <Typography variant="body1">{s.label}</Typography>
                  <Typography variant="caption" color="text.secondary">{s.description}</Typography>
                </Box>
              }
              sx={{ mb: 1 }}
            />
          ))}

          <Typography variant="subtitle2" color="text.secondary" sx={{ mt: 2, mb: 0.5 }}>
            🔄 Streaming
          </Typography>
          {SOURCE_TYPES.filter(s => s.category === 'stream').map(s => (
            <FormControlLabel
              key={s.value} value={s.value}
              control={<Radio />}
              label={
                <Box>
                  <Typography variant="body1">{s.label}</Typography>
                  <Typography variant="caption" color="text.secondary">{s.description}</Typography>
                </Box>
              }
              sx={{ mb: 1 }}
            />
          ))}

          <Typography variant="subtitle2" color="text.secondary" sx={{ mt: 2, mb: 0.5 }}>
            🌐 External
          </Typography>
          {SOURCE_TYPES.filter(s => ['api', 'database'].includes(s.category)).map(s => (
            <FormControlLabel
              key={s.value} value={s.value}
              control={<Radio />}
              label={
                <Box>
                  <Typography variant="body1">{s.label}</Typography>
                  <Typography variant="caption" color="text.secondary">{s.description}</Typography>
                </Box>
              }
              sx={{ mb: 1 }}
            />
          ))}
        </RadioGroup>
      </FormControl>
    </Box>
  );

  const renderStep3 = () => {
    if (['csv', 'json', 'parquet'].includes(sourceType)) {
      const acceptedExts = ACCEPTED_EXTENSIONS[sourceType]?.join(',') || '';
      return (
        <Box>
          <Typography variant="h6" gutterBottom>
            Upload {sourceType.toUpperCase()} Files
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Drag &amp; drop your data files below, or click to browse.
            Files will be uploaded to the pipeline when you click "Create &amp; Detect Schema".
          </Typography>

          {/* Hidden file input */}
          <input
            type="file"
            ref={fileInputRef}
            style={{ display: 'none' }}
            multiple
            accept={acceptedExts}
            onChange={handleFileSelect}
          />

          {/* Drag-and-drop zone */}
          <Paper
            variant="outlined"
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            sx={{
              p: 4,
              mb: 2,
              textAlign: 'center',
              cursor: 'pointer',
              borderStyle: 'dashed',
              borderWidth: 2,
              borderColor: isDragOver ? 'primary.main' : 'grey.400',
              bgcolor: isDragOver ? 'action.hover' : 'background.default',
              transition: 'all 0.2s ease',
              '&:hover': {
                borderColor: 'primary.main',
                bgcolor: 'action.hover',
              },
            }}
          >
            <UploadIcon sx={{ fontSize: 48, color: isDragOver ? 'primary.main' : 'grey.500', mb: 1 }} />
            <Typography variant="body1" gutterBottom>
              {isDragOver ? 'Drop files here!' : 'Drag & drop files here, or click to browse'}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Accepted: {ACCEPTED_EXTENSIONS[sourceType]?.join(', ')}
            </Typography>
          </Paper>

          {/* Selected files list */}
          {selectedFiles.length > 0 && (
            <Box sx={{ mb: 2 }}>
              <Box display="flex" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                <Typography variant="subtitle2">
                  {selectedFiles.length} file{selectedFiles.length > 1 ? 's' : ''} selected
                </Typography>
                <Chip
                  label={`Total: ${formatFileSize(selectedFiles.reduce((sum, f) => sum + f.size, 0))}`}
                  size="small"
                  color="primary"
                  variant="outlined"
                />
              </Box>
              <List dense sx={{ bgcolor: 'background.paper', borderRadius: 1, border: '1px solid', borderColor: 'divider' }}>
                {selectedFiles.map((file, idx) => (
                  <ListItem key={file.name} divider={idx < selectedFiles.length - 1}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      <FileIcon color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={file.name}
                      secondary={formatFileSize(file.size)}
                    />
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

          {/* Upload progress */}
          {uploading && (
            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Uploading files...
              </Typography>
              <LinearProgress variant="determinate" value={uploadProgress} />
            </Box>
          )}

          {/* CSV-specific options */}
          {sourceType === 'csv' && (
            <>
              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>CSV Options</Typography>
              <Grid container spacing={2}>
                <Grid item xs={4}>
                  <TextField
                    fullWidth label="Delimiter" value={csvDelimiter}
                    onChange={e => setCsvDelimiter(e.target.value)}
                    size="small"
                  />
                </Grid>
                <Grid item xs={4}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Header Row</InputLabel>
                    <Select
                      value={csvHeader ? 'yes' : 'no'}
                      label="Header Row"
                      onChange={e => setCsvHeader(e.target.value === 'yes')}
                    >
                      <MenuItem value="yes">Yes (first row is header)</MenuItem>
                      <MenuItem value="no">No (no header row)</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={4}>
                  <TextField
                    fullWidth label="Encoding" value={csvEncoding}
                    onChange={e => setCsvEncoding(e.target.value)}
                    size="small"
                  />
                </Grid>
              </Grid>
            </>
          )}

          {/* Upload result info */}
          {uploadedDir && (
            <Alert severity="success" icon={<CheckIcon />} sx={{ mt: 2 }}>
              Files uploaded to container path: <code>{uploadedDir}</code>
              {detectedFormat && <> — Detected format: <strong>{detectedFormat}</strong></>}
            </Alert>
          )}
        </Box>
      );
    }

    if (sourceType === 'api') {
      return (
        <Box>
          <Typography variant="h6" gutterBottom>Configure API Source</Typography>
          <TextField
            fullWidth label="API Endpoint" value={apiEndpoint}
            onChange={e => setApiEndpoint(e.target.value)}
            required sx={{ mb: 2 }}
            placeholder="https://api.example.com/data"
          />
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>HTTP Method</InputLabel>
            <Select value={apiMethod} label="HTTP Method" onChange={e => setApiMethod(e.target.value)}>
              <MenuItem value="GET">GET</MenuItem>
              <MenuItem value="POST">POST</MenuItem>
            </Select>
          </FormControl>
        </Box>
      );
    }

    if (sourceType === 'kafka') {
      return (
        <Box>
          <Typography variant="h6" gutterBottom>Configure Kafka Source</Typography>
          <TextField
            fullWidth label="Bootstrap Servers" value={kafkaBootstrap}
            onChange={e => setKafkaBootstrap(e.target.value)}
            required sx={{ mb: 2 }}
            placeholder="kafka:9092"
          />
          <TextField
            fullWidth label="Topic" value={kafkaTopic}
            onChange={e => setKafkaTopic(e.target.value)}
            required sx={{ mb: 2 }}
          />
        </Box>
      );
    }

    if (sourceType === 'database') {
      return (
        <Box>
          <Typography variant="h6" gutterBottom>Configure Database Source</Typography>
          <TextField
            fullWidth label="Connection String" value={dbConnection}
            onChange={e => setDbConnection(e.target.value)}
            required sx={{ mb: 2 }}
            placeholder="postgresql://user:pass@host:5432/dbname"
          />
          <TextField
            fullWidth label="Table Name" value={dbTable}
            onChange={e => setDbTable(e.target.value)}
            sx={{ mb: 2 }}
          />
        </Box>
      );
    }

    return null;
  };

  return (
    <Box maxWidth={700} mx="auto">
      <Typography variant="h4" gutterBottom>Create Pipeline</Typography>

      <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
        {STEPS.map(label => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <Card>
        <CardContent>
          {activeStep === 0 && renderStep1()}
          {activeStep === 1 && renderStep2()}
          {activeStep === 2 && renderStep3()}
        </CardContent>
      </Card>

      <Box display="flex" justifyContent="space-between" mt={3}>
        <Button disabled={activeStep === 0} onClick={handleBack}>
          Back
        </Button>
        <Button
          variant="contained"
          onClick={handleNext}
          disabled={loading}
        >
          {loading ? (uploading ? 'Uploading...' : 'Creating...') : activeStep === STEPS.length - 1 ? 'Create & Detect Schema' : 'Next'}
        </Button>
      </Box>
    </Box>
  );
}
