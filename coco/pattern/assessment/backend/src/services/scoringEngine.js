/**
 * Pattern definitions keyed by category name.
 * Each category maps to a named "Pattern" with its metadata.
 */
const PATTERNS = {
  Analytical: {
    name: 'The Strategist',
    tagline: 'Logic-driven · Data-focused · Systematic',
    description:
      'You thrive on evidence-based reasoning, structured decomposition of problems, and methodical decision-making.',
    color: '#4F46E5',
  },
  Creative: {
    name: 'The Innovator',
    tagline: 'Imaginative · Idea-rich · Boundary-pushing',
    description:
      'You excel at generating novel ideas, making surprising conceptual connections, and challenging the status quo.',
    color: '#D97706',
  },
  Structured: {
    name: 'The Architect',
    tagline: 'Process-oriented · Reliable · Precision-focused',
    description:
      'You build dependable systems, bring clarity to complexity, and deliver consistent, high-quality outcomes.',
    color: '#059669',
  },
  Adaptive: {
    name: 'The Navigator',
    tagline: 'Flexible · Resilient · Opportunity-oriented',
    description:
      'You excel at reading shifting situations, pivoting quickly, and charting pragmatic paths through ambiguity.',
    color: '#DC2626',
  },
};

/**
 * Computes a weighted score per category from submitted answers.
 *
 * Algorithm:
 *   score[category] += answer.value * question.weightage
 *
 * @param {Array} questions - Full question list from questions.json
 * @param {Array<{questionId: number, value: number}>} answers
 * @returns {Object<string, number>} scores keyed by category
 */
function calculateScores(questions, answers) {
  const scores = {};

  for (const answer of answers) {
    const question = questions.find((q) => q.id === answer.questionId);
    if (!question) continue;

    const { category, weightage } = question;
    scores[category] = (scores[category] || 0) + answer.value * weightage;
  }

  // Round to 2 decimal places for clean output
  for (const cat of Object.keys(scores)) {
    scores[cat] = Math.round(scores[cat] * 100) / 100;
  }

  return scores;
}

/**
 * Determines the winning pattern from category scores.
 * Returns the full pattern metadata + the category name and raw score.
 *
 * @param {Object<string, number>} scores
 * @returns {{ category: string, score: number, name: string, tagline: string, description: string, color: string }}
 */
function determinePattern(scores) {
  const [topCategory, topScore] = Object.entries(scores).reduce(
    (best, entry) => (entry[1] > best[1] ? entry : best),
    ['', -Infinity]
  );

  return {
    category: topCategory,
    score: topScore,
    ...PATTERNS[topCategory],
  };
}

/**
 * Returns all patterns sorted by their score (descending).
 * Useful for showing a "leaderboard" of pattern alignment.
 */
function rankPatterns(scores) {
  return Object.entries(scores)
    .sort(([, a], [, b]) => b - a)
    .map(([category, score]) => ({
      category,
      score,
      ...PATTERNS[category],
    }));
}

module.exports = { calculateScores, determinePattern, rankPatterns, PATTERNS };
