import React from 'react';
import { render, screen } from '@testing-library/react';
import App from './App';

jest.mock('./components/Layout', () => ({
  __esModule: true,
  default: ({ children }: { children: React.ReactNode }) => <div data-testid="layout">{children}</div>,
}));

jest.mock('./pages/ProjectsPage', () => ({
  __esModule: true,
  default: () => <div>Projects Page</div>,
}));

jest.mock('./pages/CreateProjectPage', () => ({
  __esModule: true,
  default: () => <div>Create Project Page</div>,
}));

jest.mock('./pages/ProjectDetailPage', () => ({
  __esModule: true,
  default: () => <div>Project Detail Page</div>,
}));

jest.mock('./pages/CreateTaskPage', () => ({
  __esModule: true,
  default: () => <div>Create Task Page</div>,
}));

jest.mock('./pages/SchemaPreviewPage', () => ({
  __esModule: true,
  default: () => <div>Schema Preview Page</div>,
}));

jest.mock('./pages/SilverEnrichmentPage', () => ({
  __esModule: true,
  default: () => <div>Silver Enrichment Page</div>,
}));

jest.mock('./pages/GoldEnrichmentPage', () => ({
  __esModule: true,
  default: () => <div>Gold Enrichment Page</div>,
}));

describe('App', () => {
  it('renders default route content', () => {
    render(<App />);
    expect(screen.getByTestId('layout')).toBeInTheDocument();
    expect(screen.getByText('Projects Page')).toBeInTheDocument();
  });
});
