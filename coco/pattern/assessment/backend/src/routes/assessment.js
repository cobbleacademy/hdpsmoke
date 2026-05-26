const express = require('express');
const router = express.Router();
const questions = require('../data/questions.json');
const { calculateScores, determinePattern, rankPatterns } = require('../services/scoringEngine');
const { generateExplanation } = require('../services/llmService');

router.get('/questions', (req, res) => {
  res.json(questions);
});

router.post('/submit', async (req, res) => {
  const { answers } = req.body;

  if (!Array.isArray(answers) || answers.length === 0) {
    return res.status(400).json({ error: 'answers must be a non-empty array' });
  }

  for (const a of answers) {
    if (typeof a.questionId !== 'number' || typeof a.value !== 'number') {
      return res
        .status(400)
        .json({ error: 'Each answer must have numeric questionId and value fields' });
    }
  }

  try {
    const scores = calculateScores(questions, answers);
    const pattern = determinePattern(scores);
    const rankedPatterns = rankPatterns(scores);

    const explanation = await generateExplanation({
      pattern,
      rankedPatterns,
      answers,
      questions,
    });

    res.json({ pattern, scores, rankedPatterns, explanation });
  } catch (err) {
    console.error('Assessment error:', err);
    res.status(500).json({ error: 'Failed to process assessment' });
  }
});

module.exports = router;
