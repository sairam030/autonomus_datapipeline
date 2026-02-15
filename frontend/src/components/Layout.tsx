import React from 'react';
import {
  AppBar, Toolbar, Typography, Box, Drawer, List,
  ListItemButton, ListItemIcon, ListItemText, Divider,
} from '@mui/material';
import {
  Dashboard as DashboardIcon,
  AddCircleOutline as AddIcon,
  Hub as HubIcon,
} from '@mui/icons-material';
import { useNavigate, useLocation } from 'react-router-dom';

const DRAWER_WIDTH = 250;

const navItems = [
  { label: 'Projects', icon: <DashboardIcon />, path: '/' },
  { label: 'New Project', icon: <AddIcon />, path: '/create' },
];

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  const navigate = useNavigate();
  const location = useLocation();

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
        <Toolbar>
          <HubIcon sx={{ mr: 1.5, color: 'primary.main' }} />
          <Typography variant="h6" noWrap sx={{ fontWeight: 700, letterSpacing: '-0.5px' }}>
            Autonomous Pipeline
          </Typography>
          <Typography variant="caption" sx={{ ml: 1.5, color: 'text.secondary', mt: 0.5 }}>
            v0.2
          </Typography>
        </Toolbar>
      </AppBar>

      <Drawer
        variant="permanent"
        sx={{
          width: DRAWER_WIDTH,
          flexShrink: 0,
          '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box' },
        }}
      >
        <Toolbar />
        <Box sx={{ overflow: 'auto', mt: 1 }}>
          <List>
            {navItems.map((item) => (
              <ListItemButton
                key={item.path}
                selected={location.pathname === item.path}
                onClick={() => navigate(item.path)}
                sx={{
                  mx: 1, borderRadius: 2, mb: 0.5,
                  '&.Mui-selected': {
                    bgcolor: 'rgba(144,202,249,0.08)',
                    '&:hover': { bgcolor: 'rgba(144,202,249,0.12)' },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 40 }}>{item.icon}</ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            ))}
          </List>
          <Divider sx={{ mx: 2, my: 1 }} />
        </Box>
      </Drawer>

      <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8 }}>
        {children}
      </Box>
    </Box>
  );
}
