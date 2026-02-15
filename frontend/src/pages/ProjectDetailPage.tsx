import React, { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography, Box, Card, CardContent, Button, Chip, Grid,
  CircularProgress, Alert, IconButton, Tooltip, Divider,
  Avatar, Paper, Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, FormControl, InputLabel, Select, MenuItem,
  List, ListItem, ListItemText, ListItemSecondaryAction,
  Tabs, Tab,
} from '@mui/material';
import {
  Add as AddIcon,
  ArrowForward as ArrowIcon,
  Storage as BronzeIcon,
  Transform as SilverIcon,
  Star as GoldIcon,
  Schedule as ScheduleIcon,
  CheckCircle as DoneIcon,
  PlayArrow as RunIcon,
  Link as LinkIcon,
  Delete as DeleteIcon,
  Code as CodeIcon,
  ArrowUpward as UpIcon,
  ArrowDownward as DownIcon,
  DragIndicator as DragIcon,
  Warning as WarningIcon,
} from '@mui/icons-material';
import {
  pipelineApi, Pipeline, schemaApi, bronzeApi,
  dagApi, DAGScheduleConfig, DAGInfo, DAGFileInfo, GenerateDAGResponse,
  TaskDAGRequest, MasterDAGRequest,
  silverApi, goldApi,
} from '../services/api';

// ============================================================================
// Task type definitions
// ============================================================================
interface Task {
  id: string;
  type: 'bronze' | 'silver' | 'gold';
  label: string;
  status: 'not_started' | 'configured' | 'running' | 'completed' | 'error';
  description: string;
  details?: Record<string, any>;
}

const TASK_ICONS: Record<string, React.ReactElement> = {
  bronze: <BronzeIcon />,
  silver: <SilverIcon />,
  gold: <GoldIcon />,
};

const TASK_COLORS: Record<string, string> = {
  bronze: '#cd7f32',
  silver: '#c0c0c0',
  gold: '#ffd700',
};

const STATUS_CHIP_COLOR: Record<string, 'default' | 'info' | 'success' | 'warning' | 'error'> = {
  not_started: 'default',
  configured: 'info',
  running: 'warning',
  completed: 'success',
  error: 'error',
};

const STATUS_LABEL: Record<string, string> = {
  not_started: 'Not Started',
  configured: 'Configured',
  running: 'Running',
  completed: 'Completed',
  error: 'Error',
};

// Schedule presets
const SCHEDULE_PRESETS = [
  { value: '', label: 'Manual only (no schedule)' },
  { value: '@hourly', label: 'Every hour' },
  { value: '0 */6 * * *', label: 'Every 6 hours' },
  { value: '@daily', label: 'Daily (midnight)' },
  { value: '0 6 * * *', label: 'Daily at 6 AM' },
  { value: '@weekly', label: 'Weekly (Sunday midnight)' },
  { value: 'custom', label: 'Custom cron expression…' },
];

export default function ProjectDetailPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();

  const [project, setProject] = useState<Pipeline | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // DAG dialog state
  const [dagDialogOpen, setDagDialogOpen] = useState(false);
  const [dagTab, setDagTab] = useState(0); // 0 = Task DAG, 1 = Master DAG

  // Task DAG form
  const [taskType, setTaskType] = useState<'bronze' | 'silver' | 'gold'>('bronze');
  const [taskLabel, setTaskLabel] = useState('');

  // Master DAG form — ordered list of task DAG IDs
  const [masterDagIds, setMasterDagIds] = useState<string[]>([]);

  // Shared schedule settings
  const [dagSchedulePreset, setDagSchedulePreset] = useState('');
  const [dagCustomCron, setDagCustomCron] = useState('');
  const [dagStartDate, setDagStartDate] = useState('2024-01-01');
  const [dagRetries, setDagRetries] = useState(1);
  const [dagRetryDelay, setDagRetryDelay] = useState(5);
  const [dagOwner, setDagOwner] = useState('autonomous-pipeline');
  const [dagGenerating, setDagGenerating] = useState(false);
  const [dagResultInfo, setDagResultInfo] = useState<DAGInfo | null>(null);
  const [dagError, setDagError] = useState<string | null>(null);

  // Delete project dialog
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [deletingProject, setDeletingProject] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const handleDeleteProject = async () => {
    if (!project || deleteConfirmText !== project.name) return;
    setDeletingProject(true);
    setDeleteError(null);
    try {
      await pipelineApi.delete(project.id);
      navigate('/');
    } catch (err: any) {
      setDeleteError(err.response?.data?.detail || 'Failed to delete project');
    } finally {
      setDeletingProject(false);
    }
  };

  // Existing DAG files
  const [existingDags, setExistingDags] = useState<DAGFileInfo[]>([]);
  const [showDagList, setShowDagList] = useState(false);

  // Derived: list of task DAG IDs already generated (for master DAG ordering)
  const taskDagIds: string[] = existingDags
    .map(d => d.filename.replace('.py', ''))
    .filter(name => !name.startsWith('master_'));

  // =========================================================================
  // Load project + derive tasks + load existing DAGs
  // =========================================================================
  useEffect(() => {
    if (!projectId) return;

    const load = async () => {
      try {
        const p = await pipelineApi.get(projectId);
        setProject(p);

        // Load existing DAGs
        try {
          const dags = await dagApi.list(projectId);
          setExistingDags(dags);
        } catch { /* no DAGs yet */ }

        // Derive tasks from pipeline state
        const derivedTasks: Task[] = [];

        // Bronze task — always exists
        let bronzeStatus: Task['status'] = 'not_started';
        let bronzeDesc = 'Upload data files and ingest to Bronze layer';
        const bronzeDetails: Record<string, any> = {};

        if (['schema_detected', 'schema_confirmed', 'bronze_ready', 'silver_configured', 'gold_configured', 'gold_ready', 'active'].includes(p.status)) {
          try {
            const versions = await schemaApi.listVersions(projectId);
            if (versions.length > 0) {
              bronzeDetails.schema_version = versions[0].version;
              bronzeDetails.fields = versions[0].fields?.length || 0;
              bronzeDetails.status_detail = versions[0].status;
            }
          } catch { /* ignore */ }
        }

        if (p.status === 'bronze_ready' || p.status === 'silver_configured' || p.status === 'gold_configured' || p.status === 'gold_ready' || p.status === 'active') {
          bronzeStatus = 'completed';
          bronzeDesc = 'Data ingested to Bronze layer';
          try {
            const ingestions = await bronzeApi.listIngestions(projectId);
            if (ingestions.length > 0) {
              bronzeDetails.total_records = ingestions[0].total_records;
              bronzeDetails.ingestion_status = ingestions[0].status;
            }
          } catch { /* ignore */ }
        } else if (p.status === 'schema_confirmed') {
          bronzeStatus = 'running';
          bronzeDesc = 'Bronze ingestion in progress...';
        } else if (p.status === 'schema_detected') {
          bronzeStatus = 'configured';
          bronzeDesc = 'Schema detected — review and confirm to ingest';
        } else if (p.status === 'draft') {
          bronzeStatus = 'not_started';

          // For API/Kafka sources, mark as configured once source is set
          if (['api', 'kafka'].includes(p.source_type)) {
            bronzeStatus = 'configured';
            bronzeDesc = `${p.source_type.toUpperCase()} source configured — ready to schedule DAG`;
          }
        }

        derivedTasks.push({
          id: 'bronze', type: 'bronze', label: 'Bronze Ingestion',
          status: bronzeStatus, description: bronzeDesc, details: bronzeDetails,
        });

        // Silver task
        const silverStatus: Task['status'] = p.status === 'silver_configured' || p.status === 'gold_configured' || p.status === 'gold_ready' || p.status === 'active'
          ? 'completed' : 'not_started';
        derivedTasks.push({
          id: 'silver', type: 'silver', label: 'Silver Transformation',
          status: silverStatus,
          description: silverStatus === 'completed'
            ? 'Data cleaned and transformed to Silver layer'
            : 'Clean, validate, and transform Bronze data',
        });

        // Gold task
        const goldStatus: Task['status'] = p.status === 'gold_configured' || p.status === 'gold_ready'
          ? 'completed' : 'not_started';
        derivedTasks.push({
          id: 'gold', type: 'gold', label: 'Gold Aggregation',
          status: goldStatus,
          description: goldStatus === 'completed'
            ? 'Data aggregated and ready for analytics'
            : 'Aggregate and prepare data for analytics',
        });

        setTasks(derivedTasks);
      } catch (err: any) {
        setError(err.response?.data?.detail || 'Failed to load project');
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [projectId]);

  // =========================================================================
  // Task actions
  // =========================================================================
  const handleTaskAction = (task: Task) => {
    if (!projectId) return;

    if (task.type === 'bronze') {
      if (task.status === 'not_started') {
        navigate(`/project/${projectId}/task/create`);
      } else {
        navigate(`/project/${projectId}/task/${task.id}/schema`);
      }
    } else if (task.type === 'silver') {
      // Create or navigate to silver transformation
      handleSilverAction();
    } else if (task.type === 'gold') {
      handleGoldAction();
    }
  };

  const handleSilverAction = async () => {
    if (!projectId) return;
    try {
      // Check if there are existing transformations
      const existing = await silverApi.list(projectId);
      if (existing.length > 0) {
        // Navigate to the first one (or most recent)
        navigate(`/project/${projectId}/silver/${existing[0].id}`);
      } else {
        // Create a new transformation
        const t = await silverApi.create(projectId, {
          name: 'Silver Enrichment',
          description: 'AI-driven data transformation',
        });
        navigate(`/project/${projectId}/silver/${t.id}`);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to open Silver transformation');
    }
  };

  const handleGoldAction = async () => {
    if (!projectId) return;
    try {
      const existing = await goldApi.list(projectId);
      if (existing.length > 0) {
        navigate(`/project/${projectId}/gold/${existing[0].id}`);
      } else {
        const t = await goldApi.create(projectId, {
          name: 'Gold Aggregation',
          description: 'AI-driven analytics transformation',
        });
        navigate(`/project/${projectId}/gold/${t.id}`);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to open Gold transformation');
    }
  };

  const getTaskActionLabel = (task: Task): string => {
    switch (task.status) {
      case 'not_started': return task.type === 'bronze' ? 'Upload Data' : 'Configure';
      case 'configured': return 'Review';
      case 'running': return 'View Progress';
      case 'completed': return 'View Details';
      case 'error': return 'Fix Error';
      default: return 'Open';
    }
  };

  // =========================================================================
  // DAG generation — Task DAG or Master DAG
  // =========================================================================
  const getScheduleValue = (): string | null => {
    if (dagSchedulePreset === 'custom') return dagCustomCron || null;
    return dagSchedulePreset || null;
  };

  const handleCreateTaskDAG = async () => {
    if (!projectId) return;
    setDagGenerating(true);
    setDagError(null);
    setDagResultInfo(null);

    const req: TaskDAGRequest = {
      task_type: taskType,
      task_label: taskLabel || undefined,
      schedule: getScheduleValue(),
      start_date: dagStartDate,
      retries: dagRetries,
      retry_delay_min: dagRetryDelay,
      owner: dagOwner,
    };

    try {
      const result = await dagApi.createTaskDag(projectId, req);
      setDagResultInfo(result);
      // Refresh DAG list
      const dags = await dagApi.list(projectId);
      setExistingDags(dags);
    } catch (err: any) {
      setDagError(err.response?.data?.detail || 'Failed to create task DAG');
    } finally {
      setDagGenerating(false);
    }
  };

  const handleCreateMasterDAG = async () => {
    if (!projectId) return;
    if (masterDagIds.length === 0) {
      setDagError('Select at least one task DAG to chain');
      return;
    }
    setDagGenerating(true);
    setDagError(null);
    setDagResultInfo(null);

    const req: MasterDAGRequest = {
      dag_ids: masterDagIds,
      schedule: getScheduleValue(),
      start_date: dagStartDate,
      retries: dagRetries,
      retry_delay_min: dagRetryDelay,
      owner: dagOwner,
    };

    try {
      const result = await dagApi.createMasterDag(projectId, req);
      setDagResultInfo(result);
      // Refresh
      const p = await pipelineApi.get(projectId);
      setProject(p);
      const dags = await dagApi.list(projectId);
      setExistingDags(dags);
    } catch (err: any) {
      setDagError(err.response?.data?.detail || 'Failed to create master DAG');
    } finally {
      setDagGenerating(false);
    }
  };

  // Master DAG ordering helpers
  const handleMoveUp = (idx: number) => {
    if (idx <= 0) return;
    setMasterDagIds(prev => {
      const arr = [...prev];
      [arr[idx - 1], arr[idx]] = [arr[idx], arr[idx - 1]];
      return arr;
    });
  };

  const handleMoveDown = (idx: number) => {
    setMasterDagIds(prev => {
      if (idx >= prev.length - 1) return prev;
      const arr = [...prev];
      [arr[idx], arr[idx + 1]] = [arr[idx + 1], arr[idx]];
      return arr;
    });
  };

  const handleToggleDagInMaster = (dagId: string) => {
    setMasterDagIds(prev =>
      prev.includes(dagId) ? prev.filter(d => d !== dagId) : [...prev, dagId]
    );
  };

  const openDagDialog = (tab: number = 0) => {
    setDagTab(tab);
    setDagResultInfo(null);
    setDagError(null);
    // Pre-populate master DAG list with all existing task DAGs in order
    if (tab === 1) {
      setMasterDagIds(taskDagIds);
    }
    setDagDialogOpen(true);
  };

  const handleDeleteAllDags = async () => {
    if (!projectId || !window.confirm('Delete all generated DAGs for this project?')) return;
    try {
      await dagApi.deleteAll(projectId);
      setExistingDags([]);
      const p = await pipelineApi.get(projectId);
      setProject(p);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete DAGs');
    }
  };

  // =========================================================================
  // Render
  // =========================================================================
  if (loading) {
    return (
      <Box display="flex" justifyContent="center" mt={10}>
        <CircularProgress />
      </Box>
    );
  }

  if (!project) {
    return (
      <Box mt={4}>
        <Alert severity="error">{error || 'Project not found'}</Alert>
        <Button sx={{ mt: 2 }} onClick={() => navigate('/')}>Back to Projects</Button>
      </Box>
    );
  }

  const bronzeDone = tasks.find(t => t.type === 'bronze')?.status === 'completed';
  const silverDone = tasks.find(t => t.type === 'silver')?.status === 'completed';
  const bronzeConfigured = tasks.find(t => t.type === 'bronze')?.status !== 'not_started';
  const hasDags = existingDags.length > 0;

  return (
    <Box maxWidth={900} mx="auto">
      {/* Project Header */}
      <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={4}>
        <Box>
          <Box display="flex" alignItems="center" gap={1.5} mb={0.5}>
            <Typography variant="h4">{project.name}</Typography>
          </Box>
          <Typography variant="body2" color="text.secondary">
            {project.description || 'No description'}
          </Typography>
        </Box>
        <Box display="flex" gap={1} alignItems="center">
          <Chip label={project.source_type.toUpperCase()} variant="outlined" size="small" />
          {project.status === 'active' && (
            <Chip label="DAG Active" color="success" size="small" />
          )}
          <Tooltip title="Delete Project">
            <IconButton size="small" color="error"
              onClick={() => { setDeleteConfirmText(''); setDeleteError(null); setDeleteDialogOpen(true); }}>
              <DeleteIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {/* Pipeline Flow Visual */}
      <Paper variant="outlined" sx={{ p: 3, mb: 4, bgcolor: 'rgba(255,255,255,0.02)' }}>
        <Typography variant="subtitle2" color="text.secondary" gutterBottom>
          Pipeline Flow
        </Typography>
        <Box display="flex" alignItems="center" justifyContent="center" gap={1} py={2}>
          {tasks.map((task, idx) => (
            <React.Fragment key={task.id}>
              <Tooltip title={`${task.label}: ${STATUS_LABEL[task.status]}`}>
                <Box
                  sx={{
                    display: 'flex', flexDirection: 'column', alignItems: 'center',
                    cursor: 'pointer', p: 1.5, borderRadius: 2,
                    transition: 'background 0.2s',
                    '&:hover': { bgcolor: 'action.hover' },
                  }}
                  onClick={() => handleTaskAction(task)}
                >
                  <Avatar
                    sx={{
                      bgcolor: task.status === 'completed' ? 'success.main'
                        : task.status === 'running' ? 'warning.main'
                        : 'rgba(255,255,255,0.08)',
                      color: task.status === 'completed' || task.status === 'running'
                        ? 'white' : TASK_COLORS[task.type],
                      width: 48, height: 48, mb: 1,
                    }}
                  >
                    {task.status === 'completed' ? <DoneIcon /> : TASK_ICONS[task.type]}
                  </Avatar>
                  <Typography variant="caption" fontWeight={600}>
                    {task.label}
                  </Typography>
                  <Chip
                    label={STATUS_LABEL[task.status]}
                    size="small"
                    color={STATUS_CHIP_COLOR[task.status]}
                    sx={{ mt: 0.5, height: 20, fontSize: '0.65rem' }}
                  />
                </Box>
              </Tooltip>
              {idx < tasks.length - 1 && (
                <ArrowIcon sx={{ color: 'text.secondary', mx: 1 }} />
              )}
            </React.Fragment>
          ))}
        </Box>
      </Paper>

      {/* Task Cards */}
      <Typography variant="h5" gutterBottom>Tasks</Typography>
      <Grid container spacing={2} sx={{ mb: 4 }}>
        {tasks.map((task) => (
          <Grid item xs={12} key={task.id}>
            <Card
              sx={{
                borderLeft: `4px solid ${TASK_COLORS[task.type]}`,
                transition: 'border-color 0.2s',
                '&:hover': { borderColor: 'primary.main' },
              }}
            >
              <CardContent>
                <Box display="flex" justifyContent="space-between" alignItems="center">
                  <Box display="flex" alignItems="center" gap={2}>
                    <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.06)', color: TASK_COLORS[task.type] }}>
                      {TASK_ICONS[task.type]}
                    </Avatar>
                    <Box>
                      <Typography variant="h6" sx={{ fontSize: '1rem' }}>
                        {task.label}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {task.description}
                      </Typography>
                      {task.details && Object.keys(task.details).length > 0 && (
                        <Box display="flex" gap={1} mt={1}>
                          {task.details.total_records != null && (
                            <Chip label={`${task.details.total_records.toLocaleString()} records`} size="small" variant="outlined" />
                          )}
                          {task.details.fields != null && (
                            <Chip label={`${task.details.fields} fields`} size="small" variant="outlined" />
                          )}
                          {task.details.schema_version != null && (
                            <Chip label={`Schema v${task.details.schema_version}`} size="small" variant="outlined" />
                          )}
                        </Box>
                      )}
                    </Box>
                  </Box>
                  <Box display="flex" alignItems="center" gap={1}>
                    <Chip
                      label={STATUS_LABEL[task.status]}
                      color={STATUS_CHIP_COLOR[task.status]}
                      size="small"
                    />
                    <Button
                      variant={task.status === 'not_started' ? 'contained' : 'outlined'}
                      size="small"
                      endIcon={<ArrowIcon />}
                      onClick={() => handleTaskAction(task)}
                      disabled={
                        (task.type === 'silver' && !bronzeDone) ||
                        (task.type === 'gold' && !silverDone)
                      }
                    >
                      {getTaskActionLabel(task)}
                    </Button>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Existing DAG Files */}
      {hasDags && (
        <Card sx={{ mb: 3, border: '1px solid rgba(144,202,249,0.2)' }}>
          <CardContent>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
              <Box display="flex" alignItems="center" gap={1}>
                <CodeIcon color="primary" />
                <Typography variant="h6" sx={{ fontSize: '1rem' }}>
                  Generated DAGs ({existingDags.length} files)
                </Typography>
              </Box>
              <Box display="flex" gap={1}>
                <Button size="small" onClick={() => setShowDagList(!showDagList)}>
                  {showDagList ? 'Hide' : 'Show Files'}
                </Button>
                <Button size="small" color="error" onClick={handleDeleteAllDags}
                  startIcon={<DeleteIcon />}>
                  Delete All
                </Button>
                <Button size="small" variant="outlined" onClick={() => openDagDialog(0)}
                  startIcon={<AddIcon />}>
                  Add Task DAG
                </Button>
                <Button size="small" variant="outlined" color="success"
                  onClick={() => openDagDialog(1)}
                  startIcon={<ScheduleIcon />}>
                  Create Master
                </Button>
              </Box>
            </Box>
            {showDagList && (
              <List dense sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 1, mt: 1 }}>
                {existingDags.map((dag, idx) => (
                  <ListItem key={dag.filename} divider={idx < existingDags.length - 1}>
                    <ListItemText
                      primary={dag.filename}
                      secondary={`${(dag.size_bytes / 1024).toFixed(1)} KB — Modified: ${new Date(dag.modified_at).toLocaleString()}`}
                    />
                  </ListItem>
                ))}
              </List>
            )}
          </CardContent>
        </Card>
      )}

      {/* Schedule DAG button (when bronze is at least configured) */}
      {bronzeConfigured && !hasDags && (
        <Card sx={{ bgcolor: 'rgba(144,202,249,0.06)', border: '1px solid rgba(144,202,249,0.2)' }}>
          <CardContent>
            <Box display="flex" alignItems="center" gap={2}>
              <ScheduleIcon color="primary" sx={{ fontSize: 32 }} />
              <Box>
                <Typography variant="h6" color="primary.main">
                  Ready to Create DAGs
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Create individual Task DAGs for each pipeline step, then a Master DAG to chain them.
                </Typography>
              </Box>
              <Box flexGrow={1} />
              <Box display="flex" gap={1}>
                <Button
                  variant="contained"
                  startIcon={<AddIcon />}
                  onClick={() => openDagDialog(0)}
                >
                  Task DAG
                </Button>
                <Button
                  variant="outlined"
                  color="success"
                  startIcon={<ScheduleIcon />}
                  onClick={() => openDagDialog(1)}
                >
                  Master DAG
                </Button>
              </Box>
            </Box>
          </CardContent>
        </Card>
      )}

      {/* Back */}
      <Box mt={3}>
        <Button onClick={() => navigate('/')}>← Back to Projects</Button>
      </Box>

      {/* ================================================================== */}
      {/* DAG CREATION DIALOG — Two Tabs: Task DAG / Master DAG            */}
      {/* ================================================================== */}
      <Dialog
        open={dagDialogOpen}
        onClose={() => { setDagDialogOpen(false); setDagResultInfo(null); setDagError(null); }}
        maxWidth="sm"
        fullWidth
        PaperProps={{ sx: { bgcolor: 'background.paper' } }}
      >
        <DialogTitle>
          <Box display="flex" alignItems="center" gap={1}>
            <ScheduleIcon color="primary" />
            <Typography variant="h6">Create Airflow DAG</Typography>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          {dagResultInfo ? (
            /* ---- Success state ---- */
            <Box>
              <Alert severity="success" sx={{ mb: 2 }}>
                {dagResultInfo.dag_type === 'master'
                  ? `Master DAG created: ${dagResultInfo.dag_id}`
                  : `Task DAG created: ${dagResultInfo.dag_id}`}
              </Alert>
              <List dense sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 1, mb: 2 }}>
                <ListItem>
                  <ListItemText
                    primary={dagResultInfo.dag_id}
                    secondary={`Type: ${dagResultInfo.dag_type} — File: ${dagResultInfo.filename}`}
                  />
                  <Chip
                    label={dagResultInfo.dag_type}
                    size="small"
                    color={dagResultInfo.dag_type === 'master' ? 'success' : 'primary'}
                    variant="outlined"
                  />
                </ListItem>
                {dagResultInfo.child_dags && dagResultInfo.child_dags.length > 0 && (
                  <ListItem>
                    <ListItemText
                      primary="Chained DAGs (in order)"
                      secondary={dagResultInfo.child_dags.join(' → ')}
                    />
                  </ListItem>
                )}
              </List>
              <Alert severity="info" variant="outlined">
                DAG is now available in Airflow. Visit <strong>localhost:8085</strong> to view & trigger it.
              </Alert>
            </Box>
          ) : (
            /* ---- Configuration form with tabs ---- */
            <Box sx={{ pt: 0 }}>
              <Tabs
                value={dagTab}
                onChange={(_, v) => {
                  setDagTab(v);
                  setDagError(null);
                  if (v === 1) setMasterDagIds(taskDagIds);
                }}
                sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
              >
                <Tab label="Task DAG" icon={<BronzeIcon sx={{ fontSize: 18 }} />} iconPosition="start" />
                <Tab label="Master DAG" icon={<ScheduleIcon sx={{ fontSize: 18 }} />} iconPosition="start" />
              </Tabs>

              {dagError && <Alert severity="error" sx={{ mb: 2 }}>{dagError}</Alert>}

              {/* ---- Tab 0: Task DAG ---- */}
              {dagTab === 0 && (
                <Box>
                  <Typography variant="subtitle2" gutterBottom>Task Type</Typography>
                  <FormControl fullWidth size="small" sx={{ mb: 2 }}>
                    <InputLabel>Task Type</InputLabel>
                    <Select
                      value={taskType}
                      label="Task Type"
                      onChange={e => setTaskType(e.target.value as any)}
                    >
                      <MenuItem value="bronze">
                        <Box display="flex" alignItems="center" gap={1}>
                          <BronzeIcon sx={{ color: '#cd7f32', fontSize: 20 }} />
                          Bronze — Ingestion
                        </Box>
                      </MenuItem>
                      <MenuItem value="silver">
                        <Box display="flex" alignItems="center" gap={1}>
                          <SilverIcon sx={{ color: '#c0c0c0', fontSize: 20 }} />
                          Silver — Transformation
                        </Box>
                      </MenuItem>
                      <MenuItem value="gold">
                        <Box display="flex" alignItems="center" gap={1}>
                          <GoldIcon sx={{ color: '#ffd700', fontSize: 20 }} />
                          Gold — Aggregation
                        </Box>
                      </MenuItem>
                    </Select>
                  </FormControl>

                  <TextField
                    fullWidth size="small" label="Task Label (optional)"
                    placeholder="e.g. csv_ingest, api_fetch, transform_v2"
                    value={taskLabel} onChange={e => setTaskLabel(e.target.value)}
                    sx={{ mb: 2 }}
                    helperText="Unique label for this task. Useful when creating multiple tasks of the same type."
                  />

                  <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
                    Creates a single <strong>{taskType}</strong> DAG for this project.
                    You can create multiple task DAGs, then chain them with a Master DAG.
                  </Alert>
                </Box>
              )}

              {/* ---- Tab 1: Master DAG ---- */}
              {dagTab === 1 && (
                <Box>
                  <Typography variant="subtitle2" gutterBottom>
                    Arrange Task DAGs — Execution Order
                  </Typography>
                  {taskDagIds.length === 0 ? (
                    <Alert severity="warning" sx={{ mb: 2 }}>
                      No task DAGs found. Create task DAGs first, then come back to create a Master DAG.
                    </Alert>
                  ) : (
                    <>
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                        Select and reorder the task DAGs. The Master DAG will execute them top-to-bottom.
                      </Typography>

                      {/* Available task DAGs — click to add/remove */}
                      <Paper variant="outlined" sx={{ mb: 2, p: 1 }}>
                        <Typography variant="caption" color="text.secondary" sx={{ px: 1 }}>
                          Click to select/deselect:
                        </Typography>
                        <Box display="flex" flexWrap="wrap" gap={0.5} sx={{ mt: 0.5, px: 1 }}>
                          {taskDagIds.map(id => (
                            <Chip
                              key={id}
                              label={id}
                              size="small"
                              variant={masterDagIds.includes(id) ? 'filled' : 'outlined'}
                              color={masterDagIds.includes(id) ? 'primary' : 'default'}
                              onClick={() => handleToggleDagInMaster(id)}
                              sx={{ cursor: 'pointer' }}
                            />
                          ))}
                        </Box>
                      </Paper>

                      {/* Ordered list with up/down controls */}
                      {masterDagIds.length > 0 && (
                        <List
                          dense
                          sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 1, mb: 2 }}
                        >
                          {masterDagIds.map((dagId, idx) => (
                            <ListItem key={dagId} divider={idx < masterDagIds.length - 1}
                              sx={{ bgcolor: 'rgba(255,255,255,0.02)' }}
                            >
                              <Box display="flex" alignItems="center" gap={1} mr={1}>
                                <DragIcon sx={{ color: 'text.disabled', fontSize: 18 }} />
                                <Chip
                                  label={idx + 1}
                                  size="small"
                                  sx={{ minWidth: 28, fontWeight: 700 }}
                                  color="primary"
                                  variant="outlined"
                                />
                              </Box>
                              <ListItemText
                                primary={dagId}
                                primaryTypographyProps={{ fontSize: '0.875rem', fontFamily: 'monospace' }}
                              />
                              <ListItemSecondaryAction>
                                <IconButton size="small" onClick={() => handleMoveUp(idx)}
                                  disabled={idx === 0}>
                                  <UpIcon fontSize="small" />
                                </IconButton>
                                <IconButton size="small" onClick={() => handleMoveDown(idx)}
                                  disabled={idx === masterDagIds.length - 1}>
                                  <DownIcon fontSize="small" />
                                </IconButton>
                                <IconButton size="small" color="error"
                                  onClick={() => handleToggleDagInMaster(dagId)}>
                                  <DeleteIcon fontSize="small" />
                                </IconButton>
                              </ListItemSecondaryAction>
                            </ListItem>
                          ))}
                        </List>
                      )}

                      {masterDagIds.length > 0 && (
                        <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
                          Execution order: <strong>{masterDagIds.join(' → ')}</strong>
                        </Alert>
                      )}
                    </>
                  )}
                </Box>
              )}

              {/* ---- Shared schedule settings ---- */}
              <Divider sx={{ my: 2 }} />
              <Typography variant="subtitle2" gutterBottom>Schedule</Typography>
              <FormControl fullWidth size="small" sx={{ mb: 2 }}>
                <InputLabel>Schedule Interval</InputLabel>
                <Select
                  value={dagSchedulePreset}
                  label="Schedule Interval"
                  onChange={e => setDagSchedulePreset(e.target.value)}
                >
                  {SCHEDULE_PRESETS.map(p => (
                    <MenuItem key={p.value} value={p.value}>{p.label}</MenuItem>
                  ))}
                </Select>
              </FormControl>

              {dagSchedulePreset === 'custom' && (
                <TextField
                  fullWidth size="small" label="Cron Expression" placeholder="0 6 * * *"
                  value={dagCustomCron} onChange={e => setDagCustomCron(e.target.value)}
                  sx={{ mb: 2 }}
                  helperText="Standard cron: minute hour day month weekday"
                />
              )}

              <Divider sx={{ my: 2 }} />
              <Typography variant="subtitle2" gutterBottom>DAG Settings</Typography>

              <Grid container spacing={2}>
                <Grid item xs={6}>
                  <TextField
                    fullWidth size="small" label="Start Date" type="date"
                    value={dagStartDate}
                    onChange={e => setDagStartDate(e.target.value)}
                    InputLabelProps={{ shrink: true }}
                  />
                </Grid>
                <Grid item xs={3}>
                  <TextField
                    fullWidth size="small" label="Retries" type="number"
                    value={dagRetries}
                    onChange={e => setDagRetries(parseInt(e.target.value) || 0)}
                    inputProps={{ min: 0, max: 10 }}
                  />
                </Grid>
                <Grid item xs={3}>
                  <TextField
                    fullWidth size="small" label="Retry Delay (min)" type="number"
                    value={dagRetryDelay}
                    onChange={e => setDagRetryDelay(parseInt(e.target.value) || 5)}
                    inputProps={{ min: 1, max: 60 }}
                  />
                </Grid>
                <Grid item xs={12}>
                  <TextField
                    fullWidth size="small" label="Owner" value={dagOwner}
                    onChange={e => setDagOwner(e.target.value)}
                  />
                </Grid>
              </Grid>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          {dagResultInfo ? (
            <Button onClick={() => { setDagDialogOpen(false); setDagResultInfo(null); }}>
              Done
            </Button>
          ) : (
            <>
              <Button onClick={() => setDagDialogOpen(false)}>Cancel</Button>
              {dagTab === 0 ? (
                <Button
                  variant="contained"
                  onClick={handleCreateTaskDAG}
                  disabled={dagGenerating}
                  startIcon={dagGenerating ? <CircularProgress size={16} /> : <AddIcon />}
                >
                  {dagGenerating ? 'Creating…' : 'Create Task DAG'}
                </Button>
              ) : (
                <Button
                  variant="contained"
                  color="success"
                  onClick={handleCreateMasterDAG}
                  disabled={dagGenerating || masterDagIds.length === 0}
                  startIcon={dagGenerating ? <CircularProgress size={16} /> : <ScheduleIcon />}
                >
                  {dagGenerating ? 'Creating…' : 'Create Master DAG'}
                </Button>
              )}
            </>
          )}
        </DialogActions>
      </Dialog>
      {/* Delete Project Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <WarningIcon color="error" />
          Delete Project
        </DialogTitle>
        <DialogContent>
          {deleteError && <Alert severity="error" sx={{ mb: 2 }}>{deleteError}</Alert>}
          <Alert severity="warning" sx={{ mb: 2 }}>
            This will permanently delete <strong>{project?.name}</strong> and all associated data
            including schemas, Bronze ingestions, Silver transformations, conversations, and execution records.
            This action <strong>cannot be undone</strong>.
          </Alert>
          <Typography variant="body2" sx={{ mb: 2 }}>
            To confirm, type the project name <strong>{project?.name}</strong> below:
          </Typography>
          <TextField
            fullWidth autoFocus
            placeholder={project?.name}
            value={deleteConfirmText}
            onChange={e => setDeleteConfirmText(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && deleteConfirmText === project?.name) handleDeleteProject();
            }}
          />
          {deleteConfirmText.length > 0 && deleteConfirmText !== project?.name && (
            <Typography variant="caption" color="error" sx={{ mt: 0.5, display: 'block' }}>
              Name does not match
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" color="error"
            onClick={handleDeleteProject}
            disabled={deletingProject || deleteConfirmText !== project?.name}
            startIcon={deletingProject ? <CircularProgress size={16} color="inherit" /> : <DeleteIcon />}>
            {deletingProject ? 'Deleting\u2026' : 'Delete Project'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
