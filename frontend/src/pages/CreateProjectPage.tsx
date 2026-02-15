import React, { useState } from 'react';
import {
  Typography, Box, Card, CardContent, TextField, Button, Alert,
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import { pipelineApi } from '../services/api';

export default function CreateProjectPage() {
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleCreate = async () => {
    if (!name.trim()) {
      setError('Project name is required');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      // Create a pipeline (= project) with a default source_type placeholder
      const project = await pipelineApi.create({
        name,
        description: description || undefined,
        source_type: 'csv', // default, will be set per-task
      });
      navigate(`/project/${project.id}`);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create project');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box maxWidth={600} mx="auto">
      <Typography variant="h4" gutterBottom>Create Project</Typography>
      <Typography variant="body2" color="text.secondary" mb={3}>
        A project groups related data pipeline tasks (Bronze → Silver → Gold).
        After creating a project you can add modular tasks inside it.
      </Typography>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      <Card>
        <CardContent sx={{ p: 3 }}>
          <TextField
            fullWidth
            label="Project Name"
            value={name}
            onChange={e => setName(e.target.value)}
            required
            sx={{ mb: 3 }}
            placeholder="e.g., Sales Analytics Pipeline"
            autoFocus
          />
          <TextField
            fullWidth
            label="Description (optional)"
            value={description}
            onChange={e => setDescription(e.target.value)}
            multiline
            rows={3}
            sx={{ mb: 3 }}
            placeholder="Describe what this project processes..."
          />
          <Box display="flex" justifyContent="space-between">
            <Button onClick={() => navigate('/')}>Cancel</Button>
            <Button
              variant="contained"
              onClick={handleCreate}
              disabled={loading}
              size="large"
            >
              {loading ? 'Creating...' : 'Create Project'}
            </Button>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
