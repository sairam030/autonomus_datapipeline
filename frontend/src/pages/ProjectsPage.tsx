import React, { useEffect, useState } from 'react';
import {
  Typography, Box, Card, CardContent, CardActions,
  Button, Chip, Grid, CircularProgress, IconButton,
  Avatar,
} from '@mui/material';
import {
  Add as AddIcon,
  Delete as DeleteIcon,
  ArrowForward as ArrowIcon,
  FolderOpen as FolderIcon,
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

const STATUS_LABELS: Record<string, string> = {
  draft: 'Draft',
  schema_detected: 'Schema Detected',
  schema_confirmed: 'Schema Confirmed',
  bronze_ready: 'Bronze Ready',
  silver_configured: 'Silver Ready',
  active: 'Active',
  paused: 'Paused',
  error: 'Error',
};

export default function ProjectsPage() {
  const navigate = useNavigate();
  const [projects, setProjects] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchProjects = async () => {
    try {
      const data = await pipelineApi.list();
      setProjects(data);
    } catch (err) {
      console.error('Failed to load projects:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchProjects(); }, []);

  const handleDelete = async (id: string) => {
    if (!window.confirm('Delete this project? All tasks and data will be removed.')) return;
    try {
      await pipelineApi.delete(id);
      setProjects(prev => prev.filter(p => p.id !== id));
    } catch (err) {
      console.error('Delete failed:', err);
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
        <Box>
          <Typography variant="h4">Projects</Typography>
          <Typography variant="body2" color="text.secondary" mt={0.5}>
            Each project contains modular tasks (Bronze, Silver, Gold) that form a complete pipeline.
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => navigate('/create')}
          size="large"
        >
          New Project
        </Button>
      </Box>

      {projects.length === 0 ? (
        <Card sx={{ textAlign: 'center', py: 8 }}>
          <CardContent>
            <FolderIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" color="text.secondary" gutterBottom>
              No projects yet
            </Typography>
            <Typography color="text.secondary" mb={3}>
              Create your first project to start building data pipelines.
            </Typography>
            <Button variant="contained" startIcon={<AddIcon />} onClick={() => navigate('/create')}>
              Create Project
            </Button>
          </CardContent>
        </Card>
      ) : (
        <Grid container spacing={2}>
          {projects.map((project) => (
            <Grid item xs={12} sm={6} md={4} key={project.id}>
              <Card
                sx={{
                  cursor: 'pointer',
                  transition: 'border-color 0.2s',
                  '&:hover': { borderColor: 'primary.main' },
                }}
                onClick={() => navigate(`/project/${project.id}`)}
              >
                <CardContent>
                  <Box display="flex" justifyContent="space-between" alignItems="flex-start" mb={1}>
                    <Box display="flex" alignItems="center" gap={1.5}>
                      <Avatar sx={{ bgcolor: 'primary.main', width: 36, height: 36, fontSize: 16 }}>
                        {project.name.charAt(0).toUpperCase()}
                      </Avatar>
                      <Typography variant="h6" sx={{ fontSize: '1.05rem' }}>
                        {project.name}
                      </Typography>
                    </Box>
                    <Chip
                      label={STATUS_LABELS[project.status] || project.status}
                      color={STATUS_COLORS[project.status] || 'default'}
                      size="small"
                    />
                  </Box>
                  <Typography color="text.secondary" variant="body2" sx={{ mb: 1.5, minHeight: 40 }}>
                    {project.description || 'No description'}
                  </Typography>
                  <Box display="flex" gap={1} alignItems="center">
                    <Chip label={project.source_type.toUpperCase()} variant="outlined" size="small" />
                    <Typography variant="caption" color="text.secondary">
                      {new Date(project.created_at).toLocaleDateString()}
                    </Typography>
                  </Box>
                </CardContent>
                <CardActions sx={{ px: 2, pb: 1.5 }}>
                  <Button size="small" endIcon={<ArrowIcon />}>
                    Open
                  </Button>
                  <Box flexGrow={1} />
                  <IconButton
                    size="small"
                    color="error"
                    onClick={(e) => { e.stopPropagation(); handleDelete(project.id); }}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </CardActions>
              </Card>
            </Grid>
          ))}
        </Grid>
      )}
    </Box>
  );
}
