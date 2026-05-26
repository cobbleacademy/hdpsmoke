require('dotenv').config();
const express = require('express');
const cors = require('cors');
const assessmentRoutes = require('./routes/assessment');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(
  cors({
    origin: process.env.FRONTEND_URL || 'http://localhost:5173',
    methods: ['GET', 'POST'],
  })
);
app.use(express.json());

app.use('/api/pattern/assessment', assessmentRoutes);

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.listen(PORT, () => {
  console.log(`Backend running on http://localhost:${PORT}`);
  console.log(`OpenAI integration: ${process.env.OPENAI_API_KEY ? 'enabled' : 'mock mode (no API key)'}`);
});
