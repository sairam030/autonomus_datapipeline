import React, { useEffect, useState } from 'react';
import {
  Typography, Box, Card, CardContent, CardActions,
  Button, Chip, Grid, CircularProgress, IconButton,
} from '@mui/material';
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  ArrowForward as ArrowIcon,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';
import { pipelineApi, Pipeline } from '../services/api';

const STATUS_COLORS: Record<string, 'default' | 'info' | 'success' | 'warning' | 'error'> = {
  draft: 'default',
  schema_detected: 'info',
  schema_confirmed: 'info',
  bronze_ready: 'success',
  silver_configured: 'success',
  active: 'success',
  paused: 'warning',
  error: 'error',
};

export default function DashboardPage() {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchPipelines = async () => {
    try {
      const data = await pipelineApi.list();
      setPipelines(data);
    } catch (err) {
      console.error('Failed to load pipelines:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchPipelines(); }, []);

  const handleDelete = async (id: string) => {
    if (!window.confirm('Delete this pipeline? This cannot be undone.')) return;
    try {
      await pipelineApi.delete(id);
      setPipelines(prev => prev.filter(p => p.id !== id));
    } catch (err) {
      console.error('Delete failed:', err);
    }
  };

  const getNextAction = (pipeline: Pipeline) => {
    switch (pipeline.status) {
      case 'draft':
        return { label: 'Configure Source', path: `/create?edit=${pipeline.id}` };
      case 'schema_detected':
        return { label: 'Review Schema', path: `/pipeline/${pipeline.id}/schema` };
      case 'schema_confirmed':
      case 'bronze_ready':
        return { label: 'View Details', path: `/pipeline/${pipeline.id}/schema` };
      default:
        return { label: 'View', path: `/pipeline/${pipeline.id}/schema` };
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" mt={10}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">Pipelines</Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => navigate('/create')}
        >
          New Pipeline
        </Button>
      </Box>

      {pipelines.length === 0 ? (
        <Card>
          <CardContent>
            <Typography color="text.secondary" textAlign="center" py={4}>
              No pipelines yet. Click "New Pipeline" to get started.
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Grid container spacing={2}>
          {pipelines.map((pipeline) => {
            const action = getNextAction(pipeline);
            return (
              <Grid item xs={12} sm={6} md={4} key={pipeline.id}>
                <Card>
                  <CardContent>
                    <Box display="flex" justifyContent="space-between" alignItems="flex-start">
                      <Typography variant="h6" gutterBottom>
                        {pipeline.name}
                      </Typography>
                      <Chip
                        label={pipeline.status.replace(/_/g, ' ')}
                        color={STATUS_COLORS[pipeline.status] || 'default'}
                        size="small"
                      />
                    </Box>
                    <Typography color="text.secondary" variant="body2" gutterBottom>
                      {pipeline.description || 'No description'}
                    </Typography>
                    <Chip
                      label={pipeline.source_type.toUpperCase()}
                      variant="outlined"
                      size="small"
                      sx={{ mt: 1 }}
                    />
                    <Typography variant="caption" display="block" mt={1} color="text.secondary">
                      Created: {new Date(pipeline.created_at).toLocaleDateString()}
                    </Typography>
                  </CardContent>
                  <CardActions>
                    <Button
                      size="small"
                      endIcon={<ArrowIcon />}
                      onClick={() => navigate(action.path)}
                    >
                      {action.label}
                    </Button>
                    <Box flexGrow={1} />
                    <IconButton
                      size="small"
                      color="error"
                      onClick={() => handleDelete(pipeline.id)}
                    >
                      <DeleteIcon />
                    </IconButton>
                  </CardActions>
                </Card>
              </Grid>
            );
          })}
        </Grid>
      )}
    </Box>
  );
}
