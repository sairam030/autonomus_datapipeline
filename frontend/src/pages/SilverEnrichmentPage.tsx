import React, { useEffect, useState, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Typography, Box, Paper, Button, TextField, CircularProgress,
  Alert, Chip, IconButton, Tooltip, Divider, Card, CardContent,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Dialog, DialogTitle, DialogContent, DialogActions,
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
} from '@mui/icons-material';
import {
  silverApi,
  SilverTransformation,
  ConversationMessage,
  DryRunResult,
} from '../services/api';

// ============================================================================
// Status colors
// ============================================================================
const STATUS_COLORS: Record<string, 'default' | 'info' | 'warning' | 'success' | 'error'> = {
  draft: 'default',
  chatting: 'info',
  code_generated: 'warning',
  code_reviewed: 'warning',
  dry_run_passed: 'success',
  confirmed: 'success',
  error: 'error',
};

const STATUS_LABELS: Record<string, string> = {
  draft: 'Draft',
  chatting: 'Chatting with AI',
  code_generated: 'Code Generated',
  code_reviewed: 'Code Reviewed',
  dry_run_passed: 'Dry-Run Passed',
  confirmed: 'Confirmed',
  error: 'Error',
};

export default function SilverEnrichmentPage() {
  const { projectId, transformId } = useParams<{
    projectId: string;
    transformId: string;
  }>();
  const navigate = useNavigate();

  // State
  const [transformation, setTransformation] = useState<SilverTransformation | null>(null);
  const [messages, setMessages] = useState<ConversationMessage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Chat
  const [chatInput, setChatInput] = useState('');
  const [chatSending, setChatSending] = useState(false);
  const chatEndRef = useRef<HTMLDivElement>(null);

  // Code editor
  const [code, setCode] = useState('');
  const [editingCode, setEditingCode] = useState(false);
  const [codeSaving, setCodeSaving] = useState(false);

  // Dry-run
  const [dryRunning, setDryRunning] = useState(false);
  const [dryRunResult, setDryRunResult] = useState<DryRunResult | null>(null);

  // Confirm
  const [confirming, setConfirming] = useState(false);

  // Clear chat confirmation dialog
  const [clearDialogOpen, setClearDialogOpen] = useState(false);

  // =========================================================================
  // Load data
  // =========================================================================
  const loadData = useCallback(async () => {
    if (!projectId || !transformId) return;
    try {
      const [t, msgs] = await Promise.all([
        silverApi.get(projectId, transformId),
        silverApi.getMessages(projectId, transformId),
      ]);
      setTransformation(t);
      setMessages(msgs);
      if (t.generated_code) {
        setCode(t.generated_code);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load transformation');
    } finally {
      setLoading(false);
    }
  }, [projectId, transformId]);

  useEffect(() => { loadData(); }, [loadData]);

  // Auto-scroll chat
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // =========================================================================
  // Chat handler
  // =========================================================================
  const handleSendMessage = async () => {
    if (!projectId || !transformId || !chatInput.trim()) return;
    const msg = chatInput.trim();
    setChatInput('');
    setChatSending(true);
    setError(null);

    // Optimistically add user message to UI
    const tempUserMsg: ConversationMessage = {
      id: `temp-${Date.now()}`,
      role: 'user',
      content: msg,
      code_block: null,
      dry_run_result: null,
      message_order: messages.length + 1,
      created_at: new Date().toISOString(),
    };
    setMessages(prev => [...prev, tempUserMsg]);

    try {
      const result = await silverApi.chat(projectId, transformId, msg);

      // Add assistant message
      const assistantMsg: ConversationMessage = {
        id: result.message_id,
        role: 'assistant',
        content: result.content,
        code_block: result.code || null,
        dry_run_result: null,
        message_order: messages.length + 2,
        created_at: new Date().toISOString(),
      };
      setMessages(prev => [...prev.filter(m => !m.id.startsWith('temp-')), tempUserMsg, assistantMsg]);

      // Update code if generated
      if (result.code) {
        setCode(result.code);
      }

      // Refresh transformation status
      const updated = await silverApi.get(projectId, transformId);
      setTransformation(updated);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to send message');
      // Remove optimistic message
      setMessages(prev => prev.filter(m => !m.id.startsWith('temp-')));
    } finally {
      setChatSending(false);
    }
  };

  // =========================================================================
  // Code handlers
  // =========================================================================
  const handleSaveCode = async () => {
    if (!projectId || !transformId || !code.trim()) return;
    setCodeSaving(true);
    setError(null);
    try {
      await silverApi.updateCode(projectId, transformId, code);
      setEditingCode(false);
      const updated = await silverApi.get(projectId, transformId);
      setTransformation(updated);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to save code');
    } finally {
      setCodeSaving(false);
    }
  };

  const handleDryRun = async () => {
    if (!projectId || !transformId) return;
    setDryRunning(true);
    setDryRunResult(null);
    setError(null);
    try {
      // Save code first if edited
      if (editingCode && code.trim()) {
        await silverApi.updateCode(projectId, transformId, code);
        setEditingCode(false);
      }
      const result = await silverApi.dryRun(projectId, transformId);
      setDryRunResult(result);
      const updated = await silverApi.get(projectId, transformId);
      setTransformation(updated);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Dry-run failed');
    } finally {
      setDryRunning(false);
    }
  };

  const handleConfirm = async () => {
    if (!projectId || !transformId || !code.trim()) return;
    setConfirming(true);
    setError(null);
    try {
      const updated = await silverApi.confirm(projectId, transformId, code);
      setTransformation(updated);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to confirm transformation');
    } finally {
      setConfirming(false);
    }
  };

  const handleClearChat = async () => {
    if (!projectId || !transformId) return;
    setClearDialogOpen(false);
    try {
      await silverApi.clearChat(projectId, transformId);
      setMessages([]);
      setCode('');
      setDryRunResult(null);
      const updated = await silverApi.get(projectId, transformId);
      setTransformation(updated);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to clear chat');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  // =========================================================================
  // Render helpers
  // =========================================================================
  const renderMessage = (msg: ConversationMessage) => {
    const isUser = msg.role === 'user';
    return (
      <Box
        key={msg.id}
        sx={{
          display: 'flex',
          justifyContent: isUser ? 'flex-end' : 'flex-start',
          mb: 2,
        }}
      >
        <Box
          sx={{
            maxWidth: '85%',
            display: 'flex',
            gap: 1.5,
            flexDirection: isUser ? 'row-reverse' : 'row',
          }}
        >
          {/* Avatar */}
          <Box
            sx={{
              width: 36, height: 36, borderRadius: '50%',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              bgcolor: isUser ? 'primary.main' : 'rgba(192,192,192,0.15)',
              color: isUser ? 'primary.contrastText' : '#c0c0c0',
              flexShrink: 0, mt: 0.5,
            }}
          >
            {isUser ? <PersonIcon sx={{ fontSize: 20 }} /> : <BotIcon sx={{ fontSize: 20 }} />}
          </Box>

          {/* Message bubble */}
          <Paper
            variant="outlined"
            sx={{
              p: 2,
              bgcolor: isUser ? 'rgba(144,202,249,0.08)' : 'rgba(255,255,255,0.03)',
              borderColor: isUser ? 'rgba(144,202,249,0.2)' : 'rgba(255,255,255,0.08)',
              borderRadius: 2,
            }}
          >
            <Typography
              variant="body2"
              sx={{
                whiteSpace: 'pre-wrap',
                lineHeight: 1.7,
                '& code': {
                  bgcolor: 'rgba(255,255,255,0.06)',
                  px: 0.5,
                  borderRadius: 0.5,
                  fontFamily: 'monospace',
                  fontSize: '0.85em',
                },
              }}
            >
              {msg.content}
            </Typography>

            {/* Code block badge */}
            {msg.code_block && (
              <Box mt={1.5}>
                <Chip
                  icon={<CodeIcon sx={{ fontSize: 16 }} />}
                  label="Code generated — see editor →"
                  size="small"
                  color="warning"
                  variant="outlined"
                  sx={{ fontSize: '0.75rem' }}
                />
              </Box>
            )}
          </Paper>
        </Box>
      </Box>
    );
  };

  // =========================================================================
  // Main render
  // =========================================================================
  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="60vh">
        <CircularProgress />
      </Box>
    );
  }

  if (!transformation) {
    return (
      <Box mt={4}>
        <Alert severity="error">{error || 'Transformation not found'}</Alert>
        <Button sx={{ mt: 2 }} onClick={() => navigate(-1)}>Go Back</Button>
      </Box>
    );
  }

  const isConfirmed = transformation.status === 'confirmed';
  const hasCode = !!code.trim();

  return (
    <Box sx={{ maxWidth: 1400, mx: 'auto', height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Box display="flex" alignItems="center" justifyContent="space-between" mb={2}>
        <Box display="flex" alignItems="center" gap={2}>
          <IconButton onClick={() => navigate(`/project/${projectId}`)}>
            <BackIcon />
          </IconButton>
          <Box>
            <Box display="flex" alignItems="center" gap={1}>
              <AIIcon sx={{ color: '#c0c0c0' }} />
              <Typography variant="h5">{transformation.name}</Typography>
            </Box>
            <Typography variant="body2" color="text.secondary">
              {transformation.description || 'Silver layer transformation'}
            </Typography>
          </Box>
        </Box>
        <Box display="flex" gap={1} alignItems="center">
          <Chip
            label={STATUS_LABELS[transformation.status] || transformation.status}
            color={STATUS_COLORS[transformation.status] || 'default'}
            size="small"
          />
          <Chip
            label={`${transformation.input_schema.length} input fields`}
            size="small"
            variant="outlined"
          />
        </Box>
      </Box>

      {error && <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert>}

      {/* Main content — 2 columns */}
      <Box sx={{ display: 'flex', gap: 2, flex: 1, minHeight: 0 }}>
        {/* LEFT: Chat panel */}
        <Paper
          variant="outlined"
          sx={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
        >
          {/* Chat header */}
          <Box
            sx={{
              p: 1.5,
              borderBottom: '1px solid',
              borderColor: 'divider',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <Box display="flex" alignItems="center" gap={1}>
              <BotIcon sx={{ color: '#c0c0c0', fontSize: 20 }} />
              <Typography variant="subtitle2">AI Assistant</Typography>
              <Chip
                label={`${messages.length} messages`}
                size="small"
                variant="outlined"
                sx={{ height: 20, fontSize: '0.7rem' }}
              />
            </Box>
            <Tooltip title="New chat (clear history)">
              <IconButton size="small" onClick={() => setClearDialogOpen(true)}>
                <ClearIcon sx={{ fontSize: 18 }} />
              </IconButton>
            </Tooltip>
          </Box>

          {/* Messages */}
          <Box
            sx={{
              flex: 1,
              overflowY: 'auto',
              p: 2,
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            {messages.length === 0 && (
              <Box
                sx={{
                  flex: 1,
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: 'text.secondary',
                  textAlign: 'center',
                  gap: 2,
                }}
              >
                <AIIcon sx={{ fontSize: 48, opacity: 0.3 }} />
                <Typography variant="body1" fontWeight={600}>
                  Describe your transformation
                </Typography>
                <Typography variant="body2" sx={{ maxWidth: 400 }}>
                  Tell the AI what you want to do with your data.
                  It will ask clarifying questions if needed, then generate PySpark code.
                </Typography>
                <Box sx={{ mt: 1 }}>
                  {['Filter rows where status is active',
                    'Add a column with category from Wikipedia API',
                    'Join with a routing table to add origin city',
                    'Normalize all string columns to lowercase',
                  ].map((suggestion, i) => (
                    <Chip
                      key={i}
                      label={suggestion}
                      size="small"
                      variant="outlined"
                      sx={{ m: 0.5, cursor: 'pointer', '&:hover': { bgcolor: 'action.hover' } }}
                      onClick={() => setChatInput(suggestion)}
                    />
                  ))}
                </Box>
              </Box>
            )}

            {messages.map(renderMessage)}

            {chatSending && (
              <Box display="flex" gap={1.5} mb={2}>
                <Box
                  sx={{
                    width: 36, height: 36, borderRadius: '50%',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    bgcolor: 'rgba(192,192,192,0.15)', color: '#c0c0c0',
                    flexShrink: 0,
                  }}
                >
                  <BotIcon sx={{ fontSize: 20 }} />
                </Box>
                <Paper variant="outlined" sx={{ p: 2, bgcolor: 'rgba(255,255,255,0.03)' }}>
                  <Box display="flex" alignItems="center" gap={1}>
                    <CircularProgress size={16} />
                    <Typography variant="body2" color="text.secondary">
                      AI is thinking…
                    </Typography>
                  </Box>
                </Paper>
              </Box>
            )}

            <div ref={chatEndRef} />
          </Box>

          {/* Chat input */}
          <Box
            sx={{
              p: 1.5,
              borderTop: '1px solid',
              borderColor: 'divider',
              display: 'flex',
              gap: 1,
            }}
          >
            <TextField
              fullWidth
              size="small"
              multiline
              maxRows={4}
              placeholder={
                messages.length === 0
                  ? 'Describe what transformation you want…'
                  : 'Follow up or refine…'
              }
              value={chatInput}
              onChange={e => setChatInput(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={chatSending || isConfirmed}
              sx={{
                '& .MuiOutlinedInput-root': {
                  borderRadius: 2,
                  bgcolor: 'rgba(255,255,255,0.03)',
                },
              }}
            />
            <Button
              variant="contained"
              onClick={handleSendMessage}
              disabled={!chatInput.trim() || chatSending || isConfirmed}
              sx={{ minWidth: 48, borderRadius: 2 }}
            >
              {chatSending ? <CircularProgress size={20} color="inherit" /> : <SendIcon />}
            </Button>
          </Box>
        </Paper>

        {/* RIGHT: Code + Dry-run panel */}
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 2, minHeight: 0 }}>
          {/* Code editor */}
          <Paper
            variant="outlined"
            sx={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
              minHeight: 200,
            }}
          >
            <Box
              sx={{
                p: 1.5,
                borderBottom: '1px solid',
                borderColor: 'divider',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <Box display="flex" alignItems="center" gap={1}>
                <CodeIcon sx={{ color: '#90caf9', fontSize: 20 }} />
                <Typography variant="subtitle2">PySpark Code</Typography>
              </Box>
              <Box display="flex" gap={0.5}>
                {hasCode && !editingCode && (
                  <Tooltip title="Edit code">
                    <IconButton size="small" onClick={() => setEditingCode(true)}>
                      <EditIcon sx={{ fontSize: 18 }} />
                    </IconButton>
                  </Tooltip>
                )}
                {editingCode && (
                  <Tooltip title="Save edits">
                    <IconButton size="small" onClick={handleSaveCode} disabled={codeSaving}>
                      {codeSaving ? <CircularProgress size={16} /> : <SaveIcon sx={{ fontSize: 18 }} />}
                    </IconButton>
                  </Tooltip>
                )}
                {hasCode && (
                  <Tooltip title="Copy code">
                    <IconButton
                      size="small"
                      onClick={() => navigator.clipboard.writeText(code)}
                    >
                      <CopyIcon sx={{ fontSize: 18 }} />
                    </IconButton>
                  </Tooltip>
                )}
              </Box>
            </Box>

            {/* Code area */}
            <Box sx={{ flex: 1, overflow: 'auto', position: 'relative' }}>
              {hasCode ? (
                editingCode ? (
                  <TextField
                    fullWidth
                    multiline
                    value={code}
                    onChange={e => setCode(e.target.value)}
                    sx={{
                      height: '100%',
                      '& .MuiOutlinedInput-root': {
                        fontFamily: '"Fira Code", "Consolas", monospace',
                        fontSize: '0.85rem',
                        lineHeight: 1.6,
                        height: '100%',
                        alignItems: 'flex-start',
                        borderRadius: 0,
                        border: 'none',
                        bgcolor: 'rgba(0,0,0,0.3)',
                        '& fieldset': { border: 'none' },
                      },
                    }}
                  />
                ) : (
                  <Box
                    component="pre"
                    sx={{
                      m: 0, p: 2,
                      fontFamily: '"Fira Code", "Consolas", monospace',
                      fontSize: '0.85rem',
                      lineHeight: 1.6,
                      bgcolor: 'rgba(0,0,0,0.3)',
                      color: '#e0e0e0',
                      overflow: 'auto',
                      height: '100%',
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-word',
                    }}
                  >
                    {code}
                  </Box>
                )
              ) : (
                <Box
                  sx={{
                    height: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    color: 'text.secondary',
                  }}
                >
                  <Typography variant="body2">
                    Code will appear here once the AI generates it…
                  </Typography>
                </Box>
              )}
            </Box>

            {/* Action buttons */}
            {hasCode && (
              <Box
                sx={{
                  p: 1.5,
                  borderTop: '1px solid',
                  borderColor: 'divider',
                  display: 'flex',
                  gap: 1,
                  justifyContent: 'flex-end',
                }}
              >
                <Button
                  variant="outlined"
                  size="small"
                  startIcon={dryRunning ? <CircularProgress size={16} /> : <RunIcon />}
                  onClick={handleDryRun}
                  disabled={dryRunning || isConfirmed}
                >
                  {dryRunning ? 'Running…' : 'Dry-Run'}
                </Button>
                <Button
                  variant="contained"
                  size="small"
                  color="success"
                  startIcon={confirming ? <CircularProgress size={16} /> : <ConfirmIcon />}
                  onClick={handleConfirm}
                  disabled={confirming || isConfirmed}
                >
                  {isConfirmed ? 'Confirmed ✓' : confirming ? 'Confirming…' : 'Confirm & Save'}
                </Button>
              </Box>
            )}
          </Paper>

          {/* Dry-run results */}
          {dryRunResult && (
            <Paper variant="outlined" sx={{ maxHeight: 300, overflow: 'auto' }}>
              <Box
                sx={{
                  p: 1.5,
                  borderBottom: '1px solid',
                  borderColor: 'divider',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  position: 'sticky',
                  top: 0,
                  bgcolor: 'background.paper',
                  zIndex: 1,
                }}
              >
                {dryRunResult.success ? (
                  <SuccessIcon sx={{ color: 'success.main', fontSize: 20 }} />
                ) : (
                  <ErrorIcon sx={{ color: 'error.main', fontSize: 20 }} />
                )}
                <Typography variant="subtitle2">
                  Dry-Run {dryRunResult.success ? 'Passed' : 'Failed'}
                </Typography>
                {dryRunResult.success && (
                  <Chip
                    label={`${dryRunResult.row_count} rows · ${dryRunResult.output_schema.length} columns`}
                    size="small"
                    variant="outlined"
                    color="success"
                    sx={{ ml: 'auto' }}
                  />
                )}
              </Box>

              {dryRunResult.error && (
                <Alert severity="error" sx={{ m: 1, borderRadius: 1 }}>
                  {dryRunResult.error}
                </Alert>
              )}

              {dryRunResult.success && dryRunResult.output_rows.length > 0 && (
                <TableContainer>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        {dryRunResult.output_schema.map(col => (
                          <TableCell key={col.name} sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                            <Box>
                              {col.name}
                              <Typography variant="caption" display="block" color="text.secondary">
                                {col.type}
                              </Typography>
                            </Box>
                          </TableCell>
                        ))}
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {dryRunResult.output_rows.slice(0, 10).map((row, idx) => (
                        <TableRow key={idx} hover>
                          {dryRunResult.output_schema.map(col => (
                            <TableCell key={col.name} sx={{ fontSize: '0.8rem', fontFamily: 'monospace' }}>
                              {row[col.name] != null ? String(row[col.name]) : '—'}
                            </TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          )}

          {/* Input schema reference */}
          {transformation.input_schema.length > 0 && !dryRunResult && (
            <Paper variant="outlined" sx={{ maxHeight: 200, overflow: 'auto' }}>
              <Box
                sx={{
                  p: 1.5,
                  borderBottom: '1px solid',
                  borderColor: 'divider',
                  position: 'sticky',
                  top: 0,
                  bgcolor: 'background.paper',
                }}
              >
                <Typography variant="subtitle2" sx={{ fontSize: '0.8rem' }}>
                  Input Schema (Bronze)
                </Typography>
              </Box>
              <Box sx={{ px: 1.5, py: 1 }}>
                {transformation.input_schema.map((field: any, i: number) => (
                  <Box
                    key={i}
                    display="flex"
                    alignItems="center"
                    gap={1}
                    py={0.3}
                  >
                    <Typography
                      variant="caption"
                      sx={{
                        fontFamily: 'monospace',
                        fontWeight: 600,
                        color: '#90caf9',
                        minWidth: 120,
                      }}
                    >
                      {field.name}
                    </Typography>
                    <Chip
                      label={field.detected_type || field.type || 'string'}
                      size="small"
                      variant="outlined"
                      sx={{ height: 18, fontSize: '0.65rem' }}
                    />
                    {field.nullable && (
                      <Typography variant="caption" color="text.secondary">
                        nullable
                      </Typography>
                    )}
                  </Box>
                ))}
              </Box>
            </Paper>
          )}
        </Box>
      </Box>

      {/* Clear chat confirmation dialog */}
      <Dialog open={clearDialogOpen} onClose={() => setClearDialogOpen(false)}>
        <DialogTitle>Start New Chat?</DialogTitle>
        <DialogContent>
          <Typography>
            This will clear the entire conversation history and reset the generated code.
            Use this when you want to try a completely different transformation approach.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setClearDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleClearChat} color="warning" variant="contained">
            Clear & Start Fresh
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
