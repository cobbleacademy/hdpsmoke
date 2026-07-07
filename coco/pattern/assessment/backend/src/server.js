require('dotenv').config();
const express = require('express');
const cors = require('cors');
const assessmentRoutes    = require('./routes/assessment');
const opaRoutes           = require('./routes/opa');
const rangerRoutes        = require('./routes/ranger');
const identityAuditRoutes = require('./routes/identityAudit');
const governanceLifecycleRoutes = require('./routes/governanceLifecycle');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(
  cors({
    origin: process.env.FRONTEND_URL || 'http://localhost:5173',
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
  })
);
app.use(express.json());

app.use('/api/pattern/assessment', assessmentRoutes);
app.use('/api/pattern/assessment', opaRoutes);
app.use('/api/pattern/assessment', rangerRoutes);
app.use('/api/pattern/assessment', identityAuditRoutes);
app.use('/api/pattern/assessment', governanceLifecycleRoutes);

// Access Control (end-user app) — same identityAuditRoutes router mounted a
// second time at a distinct root. Not a copy: Express dispatches the exact
// same handler functions for either prefix, so there's nothing to keep in
// sync. See docs/adr/0017-access-control-slim-app.md.
app.use('/api/access/control', identityAuditRoutes);

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// Group Permission Evaluation routes are an ES module (see
// docs/adr/0015-group-permission-evaluation.md) — this file is CommonJS, so
// they're loaded via a dynamic import() before the server starts listening.
(async () => {
  const { default: permissionRoutes } = await import('./routes/permission.mjs');
  app.use('/api/pattern/assessment', permissionRoutes);
  app.use('/api/access/control', permissionRoutes); // same router, second mount — see above

  app.listen(PORT, () => {
    console.log(`Backend running on http://localhost:${PORT}`);
    console.log(`OpenAI integration: ${process.env.OPENAI_API_KEY ? 'enabled' : 'mock mode (no API key)'}`);
    console.log(`Group Permission Evaluation: USE_LLM=${process.env.USE_LLM === 'true' ? 'true (LLM parsing)' : 'false (regex parsing)'}`);
    console.log(`Identity Audit: USE_LLM=${process.env.IDENTITY_AUDIT_USE_LLM === 'true' ? 'true (LLM parsing)' : 'false (regex parsing)'}`);
    const providerAuthType = (process.env.PROVIDER_AUTH_TYPE || 'none').toLowerCase();
    const providerUrls = (process.env.PROVIDER_URLS || '').split(',').filter(Boolean);
    console.log(
      `Provider API: auth=${providerAuthType}, ` +
      `${providerUrls.length} pre-configured URL(s)`
    );
  });
})();
