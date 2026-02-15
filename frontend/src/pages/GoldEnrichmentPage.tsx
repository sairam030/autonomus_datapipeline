import React, { useEffect, useState, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography, Box, Paper, Button, TextField, CircularProgress,
  Alert, Chip, IconButton, Tooltip, Divider, Card, CardContent,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Dialog, DialogTitle, DialogContent, DialogActions, List, ListItem,
  ListItemButton, ListItemText, ListItemIcon, Badge, Checkbox, FormControlLabel,
  Tabs, Tab, Collapse, MenuItem, Select, InputLabel, FormControl,
} from '@mui/material';
import {
  Send as SendIcon,
  PlayArrow as RunIcon,
  Check as ConfirmIcon,
  Refresh as ClearIcon,
  ArrowBack as BackIcon,
  AutoAwesome as AIIcon,
  Code as CodeIcon,
  SmartToy as BotIcon,
  Person as PersonIcon,
  Edit as EditIcon,
  Save as SaveIcon,
  ErrorOutline as ErrorIcon,
  CheckCircle as SuccessIcon,
  ContentCopy as CopyIcon,
  Add as AddIcon,
  History as HistoryIcon,
  CloudUpload as UploadIcon,
  Delete as DeleteIcon,
  DragIndicator as DragIcon,
  Visibility as PreviewIcon,
  Restore as RollbackIcon,
  Timeline as TimelineIcon,
  ExpandMore as ExpandIcon,
  ExpandLess as CollapseIcon,
  Storage as PostgresIcon,
} from '@mui/icons-material';
import Editor from '@monaco-editor/react';
import {
  DndContext, closestCenter, KeyboardSensor, PointerSensor, useSensor, useSensors,
  DragEndEvent,
} from '@dnd-kit/core';
import {
  arrayMove, SortableContext, sortableKeyboardCoordinates,
  verticalListSortingStrategy, useSortable,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  goldApi, silverApi,
  GoldTransformation, ConversationMessage, DryRunResult,
  DataPreview, GoldExecution, UploadToGoldResult,
  PushToPostgresResult, PostgresPushRecord,
} from '../services/api';

// ============================================================================
// Status colors
// ============================================================================
const STATUS_COLORS: Record<string, 'default' | 'info' | 'warning' | 'success' | 'error'> = {
  draft: 'default', chatting: 'info', code_generated: 'warning',
  code_reviewed: 'warning', dry_run_passed: 'success', confirmed: 'success',
  archived: 'default', error: 'error',
};
const STATUS_LABELS: Record<string, string> = {
  draft: 'Draft', chatting: 'Chatting with AI', code_generated: 'Code Generated',
  code_reviewed: 'Code Reviewed', dry_run_passed: 'Dry-Run Passed',
  confirmed: 'Confirmed', archived: 'Archived', error: 'Error',
};

// ============================================================================
// Sortable sidebar item
// ============================================================================
function SortableTransformItem({
  t, transformId, projectId, navigate, handleDelete,
}: {
  t: GoldTransformation; transformId: string | undefined;
  projectId: string | undefined; navigate: any; handleDelete: (id: string) => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: t.id });
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 100 : 'auto' as any,
  };

  return (
    <ListItem ref={setNodeRef} style={style} disablePadding
      secondaryAction={
        t.id !== transformId ? (
          <IconButton size="small" edge="end" onClick={() => handleDelete(t.id)}>
            <DeleteIcon sx={{ fontSize: 14, opacity: 0.5 }} />
          </IconButton>
        ) : null
      }
    >
      <ListItemButton
        selected={t.id === transformId}
        onClick={() => { if (t.id !== transformId) navigate(`/project/${projectId}/gold/${t.id}`); }}
        sx={{ py: 0.8 }}
      >
        <Box {...attributes} {...listeners} sx={{ cursor: 'grab', display: 'flex', mr: 0.5 }}>
          <DragIcon sx={{ fontSize: 16, color: 'text.disabled' }} />
        </Box>
        <ListItemIcon sx={{ minWidth: 28 }}>
          <Badge badgeContent={`v${t.version}`} color="warning"
            sx={{ '& .MuiBadge-badge': { fontSize: '0.55rem', height: 14, minWidth: 14 } }}>
            <CodeIcon sx={{ fontSize: 18, color: t.status === 'confirmed' ? '#ffd700' : '#666' }} />
          </Badge>
        </ListItemIcon>
        <ListItemText
          primary={t.name}
          secondary={STATUS_LABELS[t.status] || t.status}
          primaryTypographyProps={{ fontSize: '0.8rem', fontWeight: t.id === transformId ? 700 : 400, noWrap: true }}
          secondaryTypographyProps={{ fontSize: '0.65rem' }}
        />
      </ListItemButton>
    </ListItem>
  );
}

// ============================================================================
// Main page
// ============================================================================
export default function GoldEnrichmentPage() {
  const { projectId, transformId } = useParams<{ projectId: string; transformId: string }>();
  const navigate = useNavigate();

  // All transformations
  const [allTransforms, setAllTransforms] = useState<GoldTransformation[]>([]);
  const [transformation, setTransformation] = useState<GoldTransformation | null>(null);
  const [messages, setMessages] = useState<ConversationMessage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Chat
  const [chatInput, setChatInput] = useState('');
  const [chatSending, setChatSending] = useState(false);
  const chatEndRef = useRef<HTMLDivElement>(null);

  // Code
  const [code, setCode] = useState('');
  const [editingCode, setEditingCode] = useState(false);
  const [codeSaving, setCodeSaving] = useState(false);

  // Dry-run
  const [dryRunning, setDryRunning] = useState(false);
  const [dryRunResult, setDryRunResult] = useState<DryRunResult | null>(null);

  // Confirm dialog
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);
  const [confirmName, setConfirmName] = useState('');
  const [confirming, setConfirming] = useState(false);

  // Clear chat dialog
  const [clearDialogOpen, setClearDialogOpen] = useState(false);

  // New transformation dialog
  const [newTransformDialogOpen, setNewTransformDialogOpen] = useState(false);
  const [newTransformName, setNewTransformName] = useState('');
  const [creatingTransform, setCreatingTransform] = useState(false);

  // Upload to Gold
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadToGoldResult | null>(null);
  const [uploadDialogOpen, setUploadDialogOpen] = useState(false);
  const [selectedForUpload, setSelectedForUpload] = useState<string[]>([]);

  // Push to Postgres
  const [pushDialogOpen, setPushDialogOpen] = useState(false);
  const [pushTableName, setPushTableName] = useState('');
  const [pushIfExists, setPushIfExists] = useState('replace');
  const [pushing, setPushing] = useState(false);
  const [pushResult, setPushResult] = useState<PushToPostgresResult | null>(null);
  const [pushHistory, setPushHistory] = useState<PostgresPushRecord[]>([]);
  const [pushHistoryOpen, setPushHistoryOpen] = useState(false);

  // Version history
  const [versionHistory, setVersionHistory] = useState<GoldTransformation[]>([]);
  const [versionsDialogOpen, setVersionsDialogOpen] = useState(false);

  // Execution history
  const [executions, setExecutions] = useState<GoldExecution[]>([]);
  const [executionsOpen, setExecutionsOpen] = useState(false);

  // Data preview (silver & gold)
  const [previewData, setPreviewData] = useState<DataPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewDialogOpen, setPreviewDialogOpen] = useState(false);
  const [previewType, setPreviewType] = useState<'silver' | 'gold'>('silver');

  // Bottom panel tabs
  const [bottomTab, setBottomTab] = useState(0);

  // DnD sensors
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  // =========================================================================
  // Load data
  // =========================================================================
  const loadTransformList = useCallback(async () => {
    if (!projectId) return;
    try { setAllTransforms(await goldApi.list(projectId)); } catch {}
  }, [projectId]);

  const loadCurrentTransform = useCallback(async () => {
    if (!projectId || !transformId) return;
    try {
      const [t, msgs] = await Promise.all([
        goldApi.get(projectId, transformId),
        goldApi.getMessages(projectId, transformId),
      ]);
      setTransformation(t);
      setMessages(msgs);
      setCode(t.generated_code || '');
      setDryRunResult(null);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load transformation');
    } finally { setLoading(false); }
  }, [projectId, transformId]);

  useEffect(() => { loadTransformList(); loadCurrentTransform(); }, [loadTransformList, loadCurrentTransform]);

  // Auto-refresh Gold schema from latest Silver output on page load
  useEffect(() => {
    if (!projectId) return;
    goldApi.refreshSchema(projectId).catch(() => {
      // Silently ignore — Silver may not have been run yet
    });
  }, [projectId]);

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  // =========================================================================
  // Chat
  // =========================================================================
  const handleSendMessage = async () => {
    if (!projectId || !transformId || !chatInput.trim()) return;
    const msg = chatInput.trim();
    setChatInput(''); setChatSending(true); setError(null);

    const tempUserMsg: ConversationMessage = {
      id: `temp-${Date.now()}`, role: 'user', content: msg,
      code_block: null, dry_run_result: null,
      message_order: messages.length + 1, created_at: new Date().toISOString(),
    };
    setMessages(prev => [...prev, tempUserMsg]);

    try {
      const result = await goldApi.chat(projectId, transformId, msg);
      const assistantMsg: ConversationMessage = {
        id: result.message_id, role: 'assistant', content: result.content,
        code_block: result.code || null, dry_run_result: null,
        message_order: messages.length + 2, created_at: new Date().toISOString(),
      };
      setMessages(prev => [...prev.filter(m => !m.id.startsWith('temp-')), tempUserMsg, assistantMsg]);
      if (result.code) setCode(result.code);
      setTransformation(await goldApi.get(projectId, transformId));
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to send message');
      setMessages(prev => prev.filter(m => !m.id.startsWith('temp-')));
    } finally { setChatSending(false); }
  };

  // =========================================================================
  // Code handlers
  // =========================================================================
  const handleSaveCode = async () => {
    if (!projectId || !transformId || !code.trim()) return;
    setCodeSaving(true); setError(null);
    try {
      await goldApi.updateCode(projectId, transformId, code);
      setEditingCode(false);
      setTransformation(await goldApi.get(projectId, transformId));
    } catch (err: any) { setError(err.response?.data?.detail || 'Failed to save code');
    } finally { setCodeSaving(false); }
  };

  const handleDryRun = async () => {
    if (!projectId || !transformId) return;
    setDryRunning(true); setDryRunResult(null); setError(null); setBottomTab(0);
    try {
      if (editingCode && code.trim()) {
        await goldApi.updateCode(projectId, transformId, code);
        setEditingCode(false);
      }
      const result = await goldApi.dryRun(projectId, transformId);
      setDryRunResult(result);
      setTransformation(await goldApi.get(projectId, transformId));
    } catch (err: any) { setError(err.response?.data?.detail || 'Dry-run failed');
    } finally { setDryRunning(false); }
  };

  // =========================================================================
  // Confirm
  // =========================================================================
  const handleConfirmOpen = () => { setConfirmName(transformation?.name || ''); setConfirmDialogOpen(true); };

  const handleConfirm = async () => {
    if (!projectId || !transformId || !code.trim() || !confirmName.trim()) return;
    setConfirming(true); setError(null);
    try {
      const updated = await goldApi.confirm(projectId, transformId, confirmName.trim(), code);
      setTransformation(updated);
      setConfirmDialogOpen(false);
      if (updated.id !== transformId) navigate(`/project/${projectId}/gold/${updated.id}`, { replace: true });
      await loadTransformList();
    } catch (err: any) { setError(err.response?.data?.detail || 'Failed to confirm transformation');
    } finally { setConfirming(false); }
  };

  // =========================================================================
  // Clear chat
  // =========================================================================
  const handleClearChat = async () => {
    if (!projectId || !transformId) return;
    setClearDialogOpen(false);
    try {
      await goldApi.clearChat(projectId, transformId);
      setMessages([]); setCode(''); setDryRunResult(null);
      setTransformation(await goldApi.get(projectId, transformId));
    } catch (err: any) { setError(err.response?.data?.detail || 'Failed to clear chat'); }
  };

  // =========================================================================
  // Create new transform
  // =========================================================================
  const handleCreateTransform = async () => {
    if (!projectId || !newTransformName.trim()) return;
    setCreatingTransform(true);
    try {
      const t = await goldApi.create(projectId, { name: newTransformName.trim(), description: 'AI-driven gold transformation' });
      setNewTransformDialogOpen(false); setNewTransformName('');
      navigate(`/project/${projectId}/gold/${t.id}`);
      await loadTransformList();
    } catch (err: any) { setError(err.response?.data?.detail || 'Failed to create transformation');
    } finally { setCreatingTransform(false); }
  };

  // =========================================================================
  // Delete
  // =========================================================================
  const handleDelete = async (tid: string) => {
    if (!projectId) return;
    try {
      await goldApi.delete(projectId, tid);
      await loadTransformList();
      if (tid === transformId) {
        const remaining = allTransforms.filter(t => t.id !== tid);
        if (remaining.length > 0) navigate(`/project/${projectId}/gold/${remaining[0].id}`, { replace: true });
        else navigate(`/project/${projectId}`);
      }
    } catch (err: any) { setError(err.response?.data?.detail || 'Failed to delete'); }
  };

  // =========================================================================
  // Upload to Gold
  // =========================================================================
  const confirmedTransforms = allTransforms.filter(t => t.is_active && t.status === 'confirmed');

  const handleOpenUploadDialog = () => {
    setSelectedForUpload(confirmedTransforms.map(t => t.id));
    setUploadDialogOpen(true);
  };

  const handleUploadToGold = async () => {
    if (!projectId || selectedForUpload.length === 0) return;
    setUploadDialogOpen(false);
    setUploading(true); setError(null); setUploadResult(null);
    try {
      const result = await goldApi.uploadToGold(projectId, selectedForUpload);
      setUploadResult(result);
    } catch (err: any) { setError(err.response?.data?.detail || 'Upload to Gold failed');
    } finally { setUploading(false); }
  };

  // =========================================================================
  // Push to Postgres
  // =========================================================================
  const handleOpenPushDialog = () => {
    setPushTableName('');
    setPushIfExists('replace');
    setPushDialogOpen(true);
  };

  const handlePushToPostgres = async () => {
    if (!projectId || !pushTableName.trim()) return;
    setPushDialogOpen(false);
    setPushing(true); setError(null); setPushResult(null);
    try {
      const result = await goldApi.pushToPostgres(projectId, pushTableName.trim(), pushIfExists);
      setPushResult(result);
    } catch (err: any) { setError(err.response?.data?.detail || 'Push to Postgres failed');
    } finally { setPushing(false); }
  };

  const handleLoadPushHistory = async () => {
    if (!projectId) return;
    try {
      setPushHistory(await goldApi.listPostgresPushes(projectId));
      setPushHistoryOpen(true);
    } catch { setError('Failed to load push history'); }
  };

  // =========================================================================
  // Version history + rollback
  // =========================================================================
  const handleViewVersions = async () => {
    if (!projectId || !transformation) return;
    try {
      setVersionHistory(await goldApi.getVersions(projectId, transformation.task_order));
      setVersionsDialogOpen(true);
    } catch { setError('Failed to load version history'); }
  };

  const handleRollback = async (versionId: string) => {
    if (!projectId) return;
    try {
      await goldApi.rollback(projectId, versionId);
      setVersionsDialogOpen(false);
      await loadTransformList();
      navigate(`/project/${projectId}/gold/${versionId}`, { replace: true });
    } catch (err: any) { setError(err.response?.data?.detail || 'Rollback failed'); }
  };

  // =========================================================================
  // Execution history
  // =========================================================================
  const handleLoadExecutions = async () => {
    if (!projectId) return;
    try {
      setExecutions(await goldApi.listExecutions(projectId));
      setExecutionsOpen(true);
    } catch { setError('Failed to load execution history'); }
  };

  // =========================================================================
  // Data preview
  // =========================================================================
  const handlePreview = async (type: 'silver' | 'gold') => {
    if (!projectId) return;
    setPreviewType(type); setPreviewLoading(true); setPreviewData(null); setPreviewDialogOpen(true);
    try {
      const data = type === 'silver'
        ? await silverApi.preview(projectId, 50)
        : await goldApi.preview(projectId, 50);
      setPreviewData(data);
    } catch (err: any) { setError(err.response?.data?.detail || `Failed to load ${type} preview`); setPreviewDialogOpen(false);
    } finally { setPreviewLoading(false); }
  };

  // =========================================================================
  // Drag-and-drop reorder
  // =========================================================================
  const activeTransforms = allTransforms.filter(t => t.is_active && t.status !== 'archived');

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id || !projectId) return;

    const oldIdx = activeTransforms.findIndex(t => t.id === active.id);
    const newIdx = activeTransforms.findIndex(t => t.id === over.id);
    const reordered = arrayMove(activeTransforms, oldIdx, newIdx);
    setAllTransforms(prev => {
      const archived = prev.filter(t => !t.is_active || t.status === 'archived');
      return [...reordered, ...archived];
    });

    try {
      await goldApi.reorder(projectId, reordered.map((t: GoldTransformation) => t.id));
      await loadTransformList();
    } catch { setError('Failed to save reorder'); }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSendMessage(); }
  };

  // =========================================================================
  // Render helpers
  // =========================================================================
  const renderMessage = (msg: ConversationMessage) => {
    const isUser = msg.role === 'user';
    return (
      <Box key={msg.id} sx={{ display: 'flex', justifyContent: isUser ? 'flex-end' : 'flex-start', mb: 2 }}>
        <Box sx={{ maxWidth: '85%', display: 'flex', gap: 1.5, flexDirection: isUser ? 'row-reverse' : 'row' }}>
          <Box sx={{
            width: 36, height: 36, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center',
            bgcolor: isUser ? 'primary.main' : 'rgba(255,215,0,0.15)',
            color: isUser ? 'primary.contrastText' : '#ffd700', flexShrink: 0, mt: 0.5,
          }}>
            {isUser ? <PersonIcon sx={{ fontSize: 20 }} /> : <BotIcon sx={{ fontSize: 20 }} />}
          </Box>
          <Paper variant="outlined" sx={{
            p: 2, bgcolor: isUser ? 'rgba(144,202,249,0.08)' : 'rgba(255,255,255,0.03)',
            borderColor: isUser ? 'rgba(144,202,249,0.2)' : 'rgba(255,255,255,0.08)', borderRadius: 2,
          }}>
            <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', lineHeight: 1.7,
              '& code': { bgcolor: 'rgba(255,255,255,0.06)', px: 0.5, borderRadius: 0.5, fontFamily: 'monospace', fontSize: '0.85em' },
            }}>{msg.content}</Typography>
            {msg.code_block && (
              <Box mt={1.5}>
                <Chip icon={<CodeIcon sx={{ fontSize: 16 }} />} label="Code generated — see editor →"
                  size="small" color="warning" variant="outlined" sx={{ fontSize: '0.75rem' }} />
              </Box>
            )}
          </Paper>
        </Box>
      </Box>
    );
  };

  const confirmedCount = confirmedTransforms.length;

  // =========================================================================
  // Main render
  // =========================================================================
  if (loading) return <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh"><CircularProgress /></Box>;

  if (!transformation) {
    return (
      <Box mt={4}><Alert severity="error">{error || 'Transformation not found'}</Alert>
        <Button sx={{ mt: 2 }} onClick={() => navigate(-1)}>Go Back</Button></Box>
    );
  }

  const isConfirmed = transformation.status === 'confirmed';
  const hasCode = !!code.trim();

  return (
    <Box sx={{ maxWidth: 1600, mx: 'auto', height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
        <Box display="flex" alignItems="center" gap={2}>
          <IconButton onClick={() => navigate(`/project/${projectId}`)}><BackIcon /></IconButton>
          <Box>
            <Box display="flex" alignItems="center" gap={1}>
              <AIIcon sx={{ color: '#ffd700' }} />
              <Typography variant="h5">Gold Transformations</Typography>
            </Box>
            <Typography variant="body2" color="text.secondary">
              {confirmedCount} confirmed · {activeTransforms.length} total · Drag to reorder
            </Typography>
          </Box>
        </Box>
        <Box display="flex" gap={1} alignItems="center">
          <Tooltip title="Preview Silver data (input)">
            <Button size="small" variant="outlined" startIcon={<PreviewIcon />}
              onClick={() => handlePreview('silver')} sx={{ textTransform: 'none' }}>Silver</Button>
          </Tooltip>
          <Tooltip title="Preview Gold data (output)">
            <Button size="small" variant="outlined" color="warning" startIcon={<PreviewIcon />}
              onClick={() => handlePreview('gold')} sx={{ textTransform: 'none' }}>Gold</Button>
          </Tooltip>
          <Tooltip title="Execution history">
            <Button size="small" variant="outlined" startIcon={<TimelineIcon />}
              onClick={handleLoadExecutions} sx={{ textTransform: 'none' }}>History</Button>
          </Tooltip>
          <Tooltip title="Postgres push history">
            <Button size="small" variant="outlined" color="info" startIcon={<PostgresIcon />}
              onClick={handleLoadPushHistory} sx={{ textTransform: 'none' }}>Pushes</Button>
          </Tooltip>
          {confirmedCount > 0 && (
            <>
              <Button variant="contained" sx={{ bgcolor: '#b8860b', '&:hover': { bgcolor: '#996f0a' } }}
                startIcon={uploading ? <CircularProgress size={16} color="inherit" /> : <UploadIcon />}
                onClick={handleOpenUploadDialog} disabled={uploading}>
                {uploading ? 'Processing…' : 'Upload to Gold'}
              </Button>
              <Button variant="contained" color="info"
                startIcon={pushing ? <CircularProgress size={16} color="inherit" /> : <PostgresIcon />}
                onClick={handleOpenPushDialog} disabled={pushing}>
                {pushing ? 'Pushing…' : 'Push to Postgres'}
              </Button>
            </>
          )}
        </Box>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert>}

      {/* Upload result banner */}
      {uploadResult && (
        <Alert severity={uploadResult.success ? 'success' : 'error'} sx={{ mb: 2 }} onClose={() => setUploadResult(null)}>
          {uploadResult.success
            ? `✅ Uploaded to Gold! ${uploadResult.input_records} → ${uploadResult.output_records} records (${uploadResult.transformations_applied} transforms, ${uploadResult.duration_seconds}s)`
            : `❌ Upload failed: ${uploadResult.error}`}
          {uploadResult.transform_results && uploadResult.transform_results.length > 0 && (
            <Box sx={{ mt: 1, display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
              {uploadResult.transform_results.map((tr: any) => (
                <Chip key={tr.id} label={`${tr.name} v${tr.version}: ${tr.status}${tr.error ? ` — ${tr.error}` : ''}`}
                  size="small" color={tr.status === 'success' ? 'success' : 'error'} variant="outlined" sx={{ fontSize: '0.7rem' }} />
              ))}
            </Box>
          )}
        </Alert>
      )}

      {/* Push to Postgres result banner */}
      {pushResult && (
        <Alert severity={pushResult.success ? 'success' : 'error'} sx={{ mb: 2 }} onClose={() => setPushResult(null)}>
          {pushResult.success
            ? `✅ Pushed ${pushResult.records_pushed} records to Postgres table "${pushResult.table_name}" (${pushResult.duration_seconds}s)`
            : `❌ Push failed: ${pushResult.error}`}
        </Alert>
      )}

      {/* Execution history collapsible */}
      <Collapse in={executionsOpen}>
        <Paper variant="outlined" sx={{ mb: 2, maxHeight: 200, overflow: 'auto' }}>
          <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            position: 'sticky', top: 0, bgcolor: 'background.paper', zIndex: 1 }}>
            <Typography variant="subtitle2"><TimelineIcon sx={{ fontSize: 16, mr: 0.5, verticalAlign: 'text-bottom' }} />Gold Execution History</Typography>
            <IconButton size="small" onClick={() => setExecutionsOpen(false)}><CollapseIcon sx={{ fontSize: 18 }} /></IconButton>
          </Box>
          {executions.length === 0 ? (
            <Box p={2}><Typography variant="body2" color="text.secondary">No executions yet.</Typography></Box>
          ) : (
            <Table size="small">
              <TableHead><TableRow>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Status</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>In / Out</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Transforms</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Duration</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Date</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Error</TableCell>
              </TableRow></TableHead>
              <TableBody>{executions.map(ex => (
                <TableRow key={ex.id} hover>
                  <TableCell><Chip label={ex.status} size="small" color={ex.status === 'completed' ? 'success' : ex.status === 'failed' ? 'error' : 'info'}
                    sx={{ height: 20, fontSize: '0.7rem' }} /></TableCell>
                  <TableCell sx={{ fontSize: '0.8rem' }}>{ex.input_records} → {ex.output_records}</TableCell>
                  <TableCell sx={{ fontSize: '0.8rem' }}>{ex.transformations_applied}</TableCell>
                  <TableCell sx={{ fontSize: '0.8rem' }}>{ex.duration_seconds?.toFixed(1)}s</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem' }}>{ex.started_at ? new Date(ex.started_at).toLocaleString() : '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', color: 'error.main', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {ex.error || '—'}</TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          )}
        </Paper>
      </Collapse>

      {/* Postgres push history collapsible */}
      <Collapse in={pushHistoryOpen}>
        <Paper variant="outlined" sx={{ mb: 2, maxHeight: 200, overflow: 'auto' }}>
          <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            position: 'sticky', top: 0, bgcolor: 'background.paper', zIndex: 1 }}>
            <Typography variant="subtitle2"><PostgresIcon sx={{ fontSize: 16, mr: 0.5, verticalAlign: 'text-bottom' }} />Postgres Push History</Typography>
            <IconButton size="small" onClick={() => setPushHistoryOpen(false)}><CollapseIcon sx={{ fontSize: 18 }} /></IconButton>
          </Box>
          {pushHistory.length === 0 ? (
            <Box p={2}><Typography variant="body2" color="text.secondary">No pushes yet.</Typography></Box>
          ) : (
            <Table size="small">
              <TableHead><TableRow>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Status</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Table</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Records</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Duration</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Date</TableCell>
                <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>Error</TableCell>
              </TableRow></TableHead>
              <TableBody>{pushHistory.map(p => (
                <TableRow key={p.id} hover>
                  <TableCell><Chip label={p.status} size="small" color={p.status === 'completed' ? 'success' : p.status === 'failed' ? 'error' : 'info'}
                    sx={{ height: 20, fontSize: '0.7rem' }} /></TableCell>
                  <TableCell sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>{p.table_name}</TableCell>
                  <TableCell sx={{ fontSize: '0.8rem' }}>{p.records_pushed}</TableCell>
                  <TableCell sx={{ fontSize: '0.8rem' }}>{p.duration_seconds?.toFixed(1)}s</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem' }}>{p.created_at ? new Date(p.created_at).toLocaleString() : '—'}</TableCell>
                  <TableCell sx={{ fontSize: '0.75rem', color: 'error.main', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {p.error || '—'}</TableCell>
                </TableRow>
              ))}</TableBody>
            </Table>
          )}
        </Paper>
      </Collapse>

      {/* Main 3-column layout */}
      <Box sx={{ display: 'flex', gap: 2, flex: 1, minHeight: 0 }}>

        {/* LEFT SIDEBAR */}
        <Paper variant="outlined" sx={{ width: 240, flexShrink: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="subtitle2" sx={{ fontSize: '0.8rem' }}>Gold Transforms</Typography>
            <Tooltip title="Add new transformation">
              <IconButton size="small" onClick={() => setNewTransformDialogOpen(true)}><AddIcon sx={{ fontSize: 18 }} /></IconButton>
            </Tooltip>
          </Box>
          <List dense sx={{ flex: 1, overflow: 'auto', py: 0 }}>
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
              <SortableContext items={activeTransforms.map(t => t.id)} strategy={verticalListSortingStrategy}>
                {activeTransforms.map(t => (
                  <SortableTransformItem key={t.id} t={t} transformId={transformId}
                    projectId={projectId} navigate={navigate} handleDelete={handleDelete} />
                ))}
              </SortableContext>
            </DndContext>
          </List>
        </Paper>

        {/* CENTER: Chat panel */}
        <Paper variant="outlined" sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box display="flex" alignItems="center" gap={1}>
              <BotIcon sx={{ color: '#ffd700', fontSize: 20 }} />
              <Typography variant="subtitle2">{transformation.name}</Typography>
              <Chip label={`v${transformation.version}`} size="small" variant="outlined" sx={{ height: 18, fontSize: '0.65rem' }} />
              <Chip label={STATUS_LABELS[transformation.status] || transformation.status}
                color={STATUS_COLORS[transformation.status] || 'default'} size="small" sx={{ height: 20, fontSize: '0.65rem' }} />
            </Box>
            <Box display="flex" gap={0.5}>
              {transformation.version > 1 && (
                <Tooltip title="Version history"><IconButton size="small" onClick={handleViewVersions}><HistoryIcon sx={{ fontSize: 18 }} /></IconButton></Tooltip>
              )}
              <Tooltip title="New chat"><IconButton size="small" onClick={() => setClearDialogOpen(true)}><ClearIcon sx={{ fontSize: 18 }} /></IconButton></Tooltip>
            </Box>
          </Box>

          <Box sx={{ flex: 1, overflowY: 'auto', p: 2, display: 'flex', flexDirection: 'column' }}>
            {messages.length === 0 && (
              <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'text.secondary', textAlign: 'center', gap: 2 }}>
                <AIIcon sx={{ fontSize: 48, opacity: 0.3, color: '#ffd700' }} />
                <Typography variant="body1" fontWeight={600}>Describe your Gold transformation</Typography>
                <Typography variant="body2" sx={{ maxWidth: 400 }}>
                  Tell the AI what analytics-ready transformation you want. It generates PySpark code that runs on your Silver data.
                </Typography>
                <Box sx={{ mt: 1 }}>
                  {['Aggregate flights by airline and count total',
                    'Calculate average delay per route',
                    'Group by origin city and compute statistics',
                    'Create a daily summary with counts and averages',
                  ].map((s, i) => (
                    <Chip key={i} label={s} size="small" variant="outlined"
                      sx={{ m: 0.5, cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
                      onClick={() => setChatInput(s)} />
                  ))}
                </Box>
              </Box>
            )}
            {messages.map(renderMessage)}
            {chatSending && (
              <Box display="flex" gap={1.5} mb={2}>
                <Box sx={{ width: 36, height: 36, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center',
                  bgcolor: 'rgba(255,215,0,0.15)', color: '#ffd700', flexShrink: 0 }}>
                  <BotIcon sx={{ fontSize: 20 }} />
                </Box>
                <Paper variant="outlined" sx={{ p: 2, bgcolor: 'rgba(255,255,255,0.03)' }}>
                  <Box display="flex" alignItems="center" gap={1}>
                    <CircularProgress size={16} /><Typography variant="body2" color="text.secondary">AI is thinking…</Typography>
                  </Box>
                </Paper>
              </Box>
            )}
            <div ref={chatEndRef} />
          </Box>

          <Box sx={{ p: 1.5, borderTop: '1px solid', borderColor: 'divider', display: 'flex', gap: 1 }}>
            <TextField fullWidth size="small" multiline maxRows={4}
              placeholder={messages.length === 0 ? 'Describe what Gold transformation you want…' : 'Follow up or refine…'}
              value={chatInput} onChange={e => setChatInput(e.target.value)}
              onKeyDown={handleKeyDown} disabled={chatSending}
              sx={{ '& .MuiOutlinedInput-root': { borderRadius: 2, bgcolor: 'rgba(255,255,255,0.03)' } }} />
            <Button variant="contained" onClick={handleSendMessage}
              disabled={!chatInput.trim() || chatSending} sx={{ minWidth: 48, borderRadius: 2 }}>
              {chatSending ? <CircularProgress size={20} color="inherit" /> : <SendIcon />}
            </Button>
          </Box>
        </Paper>

        {/* RIGHT: Monaco code editor + Dry-run results */}
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 2, minHeight: 0 }}>
          {/* Monaco Code Editor */}
          <Paper variant="outlined" sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', minHeight: 200 }}>
            <Box sx={{ p: 1.5, borderBottom: '1px solid', borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Box display="flex" alignItems="center" gap={1}>
                <CodeIcon sx={{ color: '#ffd700', fontSize: 20 }} />
                <Typography variant="subtitle2">PySpark Code</Typography>
              </Box>
              <Box display="flex" gap={0.5}>
                {hasCode && (
                  <>
                    <Tooltip title={editingCode ? "Lock editor" : "Edit code"}>
                      <IconButton size="small" onClick={() => setEditingCode(!editingCode)}>
                        {editingCode ? <SaveIcon sx={{ fontSize: 18 }} /> : <EditIcon sx={{ fontSize: 18 }} />}
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Copy code">
                      <IconButton size="small" onClick={() => navigator.clipboard.writeText(code)}>
                        <CopyIcon sx={{ fontSize: 18 }} />
                      </IconButton>
                    </Tooltip>
                  </>
                )}
              </Box>
            </Box>

            <Box sx={{ flex: 1, overflow: 'hidden' }}>
              {hasCode ? (
                <Editor
                  height="100%"
                  language="python"
                  theme="vs-dark"
                  value={code}
                  onChange={(v: string | undefined) => { if (editingCode && v !== undefined) setCode(v); }}
                  options={{
                    readOnly: !editingCode,
                    minimap: { enabled: false },
                    fontSize: 13,
                    lineNumbers: 'on',
                    scrollBeyondLastLine: false,
                    wordWrap: 'on',
                    automaticLayout: true,
                    padding: { top: 8 },
                    renderLineHighlight: editingCode ? 'all' : 'none',
                    cursorStyle: editingCode ? 'line' : 'line-thin',
                  }}
                />
              ) : (
                <Box sx={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'text.secondary' }}>
                  <Typography variant="body2">Code will appear here once the AI generates it…</Typography>
                </Box>
              )}
            </Box>

            {hasCode && (
              <Box sx={{ p: 1.5, borderTop: '1px solid', borderColor: 'divider', display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                {editingCode && (
                  <Button variant="outlined" size="small" onClick={handleSaveCode} disabled={codeSaving}
                    startIcon={codeSaving ? <CircularProgress size={16} /> : <SaveIcon />}>Save</Button>
                )}
                <Button variant="outlined" size="small"
                  startIcon={dryRunning ? <CircularProgress size={16} /> : <RunIcon />}
                  onClick={handleDryRun} disabled={dryRunning}>
                  {dryRunning ? 'Running…' : 'Dry-Run'}
                </Button>
                <Button variant="contained" size="small" sx={{ bgcolor: '#b8860b', '&:hover': { bgcolor: '#996f0a' } }}
                  startIcon={confirming ? <CircularProgress size={16} /> : <ConfirmIcon />}
                  onClick={handleConfirmOpen} disabled={confirming}>
                  {isConfirmed ? `Confirmed v${transformation.version}` : 'Confirm & Save'}
                </Button>
              </Box>
            )}
          </Paper>

          {/* Bottom panel: Dry-run results OR Input schema */}
          <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
            <Box sx={{ borderBottom: '1px solid', borderColor: 'divider', position: 'sticky', top: 0, bgcolor: 'background.paper', zIndex: 1 }}>
              <Tabs value={bottomTab} onChange={(_, v) => setBottomTab(v)} sx={{ minHeight: 36 }}>
                <Tab label={dryRunResult ? (dryRunResult.success ? '✅ Dry-Run' : '❌ Dry-Run') : 'Dry-Run'}
                  sx={{ minHeight: 36, fontSize: '0.75rem', textTransform: 'none' }} />
                <Tab label={`Schema (${transformation.input_schema.length} cols)`}
                  sx={{ minHeight: 36, fontSize: '0.75rem', textTransform: 'none' }} />
              </Tabs>
            </Box>

            {bottomTab === 0 && dryRunResult && (
              <Box>
                {dryRunResult.success && (
                  <Box sx={{ px: 1.5, py: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Chip label={`${dryRunResult.row_count} rows · ${dryRunResult.output_schema.length} columns`}
                      size="small" variant="outlined" color="success" />
                  </Box>
                )}
                {dryRunResult.error && <Alert severity="error" sx={{ m: 1, borderRadius: 1 }}>{dryRunResult.error}</Alert>}
                {dryRunResult.success && dryRunResult.output_rows.length > 0 && (
                  <TableContainer>
                    <Table size="small" stickyHeader>
                      <TableHead><TableRow>
                        {dryRunResult.output_schema.map(col => (
                          <TableCell key={col.name} sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                            <Box>{col.name}<Typography variant="caption" display="block" color="text.secondary">{col.type}</Typography></Box>
                          </TableCell>
                        ))}
                      </TableRow></TableHead>
                      <TableBody>{dryRunResult.output_rows.slice(0, 10).map((row, idx) => (
                        <TableRow key={idx} hover>
                          {dryRunResult.output_schema.map(col => (
                            <TableCell key={col.name} sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>
                              {row[col.name] != null ? String(row[col.name]) : '—'}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}</TableBody>
                    </Table>
                  </TableContainer>
                )}
              </Box>
            )}
            {bottomTab === 0 && !dryRunResult && (
              <Box p={2}><Typography variant="body2" color="text.secondary">Run a dry-run to see results here.</Typography></Box>
            )}

            {bottomTab === 1 && transformation.input_schema.length > 0 && (
              <Box sx={{ px: 1.5, py: 1 }}>
                {transformation.input_schema.map((field: any, i: number) => (
                  <Box key={i} display="flex" alignItems="center" gap={1} py={0.3}>
                    <Typography variant="caption" sx={{ fontFamily: 'monospace', fontWeight: 600, color: '#ffd700', minWidth: 120 }}>{field.name}</Typography>
                    <Chip label={field.detected_type || field.type || 'string'} size="small" variant="outlined" sx={{ height: 18, fontSize: '0.65rem' }} />
                    {field.nullable && <Typography variant="caption" color="text.secondary">nullable</Typography>}
                  </Box>
                ))}
              </Box>
            )}
            {bottomTab === 1 && transformation.input_schema.length === 0 && (
              <Box p={2}><Typography variant="body2" color="text.secondary">No schema available. Upload to Silver first.</Typography></Box>
            )}
          </Paper>
        </Box>
      </Box>

      {/* ======================== DIALOGS ======================== */}

      {/* Confirm dialog */}
      <Dialog open={confirmDialogOpen} onClose={() => setConfirmDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>{isConfirmed ? 'Save New Version' : 'Confirm Gold Transformation'}</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {isConfirmed ? `This will create version ${transformation.version + 1}. The previous version will be archived.` : 'Give this transformation a name and confirm the code.'}
          </Typography>
          <TextField fullWidth autoFocus label="Transformation Name" value={confirmName}
            onChange={e => setConfirmName(e.target.value)} placeholder="e.g., Aggregate by Route" sx={{ mb: 1 }} />
          {isConfirmed && <Chip label={`Current: v${transformation.version} → New: v${transformation.version + 1}`} size="small" color="info" variant="outlined" />}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleConfirm} variant="contained" sx={{ bgcolor: '#b8860b', '&:hover': { bgcolor: '#996f0a' } }} disabled={confirming || !confirmName.trim()}>
            {confirming ? <CircularProgress size={18} /> : isConfirmed ? 'Save as New Version' : 'Confirm & Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Clear chat dialog */}
      <Dialog open={clearDialogOpen} onClose={() => setClearDialogOpen(false)}>
        <DialogTitle>Start New Chat?</DialogTitle>
        <DialogContent><Typography>This will clear the conversation history and reset the generated code.</Typography></DialogContent>
        <DialogActions>
          <Button onClick={() => setClearDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleClearChat} color="warning" variant="contained">Clear & Start Fresh</Button>
        </DialogActions>
      </Dialog>

      {/* New transformation dialog */}
      <Dialog open={newTransformDialogOpen} onClose={() => setNewTransformDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>New Gold Transformation</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>Add another Gold transformation step. Transformations run on Silver data in order.</Typography>
          <TextField fullWidth autoFocus label="Transformation Name" value={newTransformName}
            onChange={e => setNewTransformName(e.target.value)} placeholder="e.g., Flight Delay Summary, Route Statistics" />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setNewTransformDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleCreateTransform} variant="contained" disabled={creatingTransform || !newTransformName.trim()}>
            {creatingTransform ? <CircularProgress size={18} /> : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Upload to Gold selection dialog */}
      <Dialog open={uploadDialogOpen} onClose={() => setUploadDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Upload to Gold</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>Select the Gold transformations to apply to Silver data.</Typography>
          <FormControlLabel
            control={<Checkbox checked={selectedForUpload.length === confirmedTransforms.length && confirmedTransforms.length > 0}
              indeterminate={selectedForUpload.length > 0 && selectedForUpload.length < confirmedTransforms.length}
              onChange={() => { if (selectedForUpload.length === confirmedTransforms.length) setSelectedForUpload([]); else setSelectedForUpload(confirmedTransforms.map(t => t.id)); }} />}
            label={<Typography variant="subtitle2">Select All</Typography>}
            sx={{ mb: 1, borderBottom: '1px solid', borderColor: 'divider', width: '100%', pb: 1 }} />
          <List dense sx={{ py: 0 }}>
            {confirmedTransforms.map((t, idx) => (
              <ListItem key={t.id} disablePadding>
                <ListItemButton onClick={() => setSelectedForUpload(prev => prev.includes(t.id) ? prev.filter(x => x !== t.id) : [...prev, t.id])} dense>
                  <Checkbox edge="start" checked={selectedForUpload.includes(t.id)} disableRipple sx={{ mr: 1 }} />
                  <ListItemText primary={`${idx + 1}. ${t.name}`} secondary={`v${t.version} · Confirmed`}
                    primaryTypographyProps={{ fontSize: '0.9rem' }} secondaryTypographyProps={{ fontSize: '0.7rem' }} />
                  <Chip label={`v${t.version}`} size="small" color="success" variant="outlined" sx={{ height: 20, fontSize: '0.65rem' }} />
                </ListItemButton>
              </ListItem>
            ))}
          </List>
          {confirmedTransforms.length === 0 && <Alert severity="warning" sx={{ mt: 1 }}>No confirmed transformations.</Alert>}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setUploadDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleUploadToGold} variant="contained" sx={{ bgcolor: '#b8860b', '&:hover': { bgcolor: '#996f0a' } }}
            disabled={selectedForUpload.length === 0} startIcon={<UploadIcon />}>
            Upload {selectedForUpload.length} Transform{selectedForUpload.length !== 1 ? 's' : ''}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Push to Postgres dialog */}
      <Dialog open={pushDialogOpen} onClose={() => setPushDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>
          <Box display="flex" alignItems="center" gap={1}>
            <PostgresIcon color="info" />
            Push Gold Data to PostgreSQL
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Load the latest Gold data into a PostgreSQL table. This makes it available for Metabase dashboards and SQL queries.
          </Typography>
          <TextField fullWidth autoFocus label="Table Name" value={pushTableName}
            onChange={e => setPushTableName(e.target.value)} placeholder="e.g., gold_flight_summary"
            helperText="The table will be created in the pipeline database" sx={{ mb: 2 }} />
          <FormControl fullWidth size="small">
            <InputLabel>If Table Exists</InputLabel>
            <Select value={pushIfExists} onChange={e => setPushIfExists(e.target.value)} label="If Table Exists">
              <MenuItem value="replace">Replace (drop & recreate)</MenuItem>
              <MenuItem value="append">Append (add rows)</MenuItem>
              <MenuItem value="fail">Fail (error if exists)</MenuItem>
            </Select>
          </FormControl>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPushDialogOpen(false)}>Cancel</Button>
          <Button onClick={handlePushToPostgres} variant="contained" color="info"
            disabled={!pushTableName.trim()} startIcon={<PostgresIcon />}>
            Push to Postgres
          </Button>
        </DialogActions>
      </Dialog>

      {/* Version history dialog with rollback */}
      <Dialog open={versionsDialogOpen} onClose={() => setVersionsDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Version History</DialogTitle>
        <DialogContent>
          <List dense>
            {versionHistory.map(v => (
              <ListItem key={v.id} disablePadding secondaryAction={
                !v.is_active && v.status === 'archived' ? (
                  <Tooltip title="Rollback to this version">
                    <IconButton size="small" color="warning" onClick={() => handleRollback(v.id)}>
                      <RollbackIcon sx={{ fontSize: 18 }} />
                    </IconButton>
                  </Tooltip>
                ) : null
              }>
                <ListItemButton onClick={() => { setVersionsDialogOpen(false); navigate(`/project/${projectId}/gold/${v.id}`); }}>
                  <ListItemText primary={`v${v.version} — ${v.name}`}
                    secondary={`${STATUS_LABELS[v.status] || v.status} · ${new Date(v.created_at).toLocaleString()}`}
                    primaryTypographyProps={{ fontWeight: v.is_active ? 700 : 400 }} />
                  <Box display="flex" gap={0.5}>
                    {v.is_active && <Chip label="active" size="small" color="success" variant="outlined" sx={{ height: 20, fontSize: '0.65rem' }} />}
                    <Chip label={STATUS_LABELS[v.status]} size="small" color={STATUS_COLORS[v.status] || 'default'} sx={{ height: 20, fontSize: '0.65rem' }} />
                  </Box>
                </ListItemButton>
              </ListItem>
            ))}
          </List>
        </DialogContent>
        <DialogActions><Button onClick={() => setVersionsDialogOpen(false)}>Close</Button></DialogActions>
      </Dialog>

      {/* Data preview dialog */}
      <Dialog open={previewDialogOpen} onClose={() => setPreviewDialogOpen(false)} maxWidth="lg" fullWidth
        PaperProps={{ sx: { bgcolor: 'background.paper', maxHeight: '80vh' } }}>
        <DialogTitle>
          <Box display="flex" alignItems="center" gap={1}>
            <PreviewIcon color={previewType === 'silver' ? 'secondary' : 'warning'} />
            <Typography variant="h6">{previewType === 'silver' ? 'Silver' : 'Gold'} Data Preview</Typography>
            {previewData && <Chip label={`${previewData.total_records.toLocaleString()} total records`} size="small" variant="outlined" />}
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          {previewLoading ? (
            <Box display="flex" justifyContent="center" py={4}><CircularProgress /><Typography sx={{ ml: 2 }}>Loading preview…</Typography></Box>
          ) : previewData ? (
            <TableContainer sx={{ maxHeight: '60vh' }}>
              <Table size="small" stickyHeader>
                <TableHead><TableRow>
                  {previewData.columns.map(col => (
                    <TableCell key={col.name} sx={{ fontWeight: 700, fontSize: '0.75rem', bgcolor: 'background.paper' }}>
                      <Box>{col.name}<Typography variant="caption" display="block" color="text.secondary">{col.type}</Typography></Box>
                    </TableCell>
                  ))}
                </TableRow></TableHead>
                <TableBody>{previewData.rows.map((row, idx) => (
                  <TableRow key={idx} hover>
                    {previewData.columns.map(col => (
                      <TableCell key={col.name} sx={{ fontSize: '0.8rem', fontFamily: 'monospace', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row[col.name] != null ? String(row[col.name]) : '—'}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}</TableBody>
              </Table>
            </TableContainer>
          ) : (
            <Typography color="text.secondary">No data available.</Typography>
          )}
        </DialogContent>
        <DialogActions>
          {previewData && (
            <Typography variant="caption" color="text.secondary" sx={{ mr: 'auto', ml: 1 }}>
              Showing {previewData.preview_count} of {previewData.total_records.toLocaleString()} rows
            </Typography>
          )}
          <Button onClick={() => setPreviewDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
