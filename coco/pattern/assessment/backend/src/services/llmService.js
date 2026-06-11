const { getClient, isLlmConfigured } = require('./llmClient');

/**
 * Builds the prompt sent to the LLM.
 * The template is structured so the model has:
 *  1. Full scoring context (which category won and by how much)
 *  2. The user's raw answer-by-answer trace for evidence
 *  3. Clear output requirements (3 paragraphs, warm tone, specific references)
 */
function buildPrompt({ pattern, rankedPatterns, answers, questions }) {
  const answerLines = answers
    .map((answer) => {
      const q = questions.find((x) => x.id === answer.questionId);
      const opt = q?.options.find((o) => o.val === answer.value);
      return `  • [${q?.category}] "${q?.text}"\n    → Selected: "${opt?.label}" (value ${answer.value} × weightage ${q?.weightage} = ${(answer.value * q?.weightage).toFixed(2)} pts)`;
    })
    .join('\n');

  const scoreLines = rankedPatterns
    .map(
      (p, i) =>
        `  ${i + 1}. ${p.name} (${p.category}): ${p.score.toFixed(2)} pts${i === 0 ? ' ← winner' : ''}`
    )
    .join('\n');

  return `You are a professional cognitive-style and personality analyst. A user has just completed a structured Pattern Assessment quiz.

## Final Scores (highest = recommended pattern)
${scoreLines}

## Recommended Pattern
**${pattern.name}** — "${pattern.tagline}"
${pattern.description}

## User's Response Trace
${answerLines}

## Your Task
Write a personalized assessment in exactly 3 paragraphs:

**Paragraph 1 — Why This Pattern Fits You**
Explain specifically why this person fits the "${pattern.name}" pattern. Reference 2–3 of their actual answers by name as concrete evidence. Be specific, not generic.

**Paragraph 2 — Your Strengths & Secondary Patterns**
Describe 3–4 concrete strengths this pattern brings. If any other category scored within 20% of the winner, acknowledge that secondary trait as a meaningful complement. Ground this in their actual score profile.

**Paragraph 3 — One Actionable Insight**
Give a single, specific, practical tip for how they can leverage this pattern more effectively in their work or daily life. Make it concrete enough that they could act on it today.

**Rules:**
- Address the user directly using "you" throughout
- Do NOT use bullet points inside paragraphs — prose only
- Avoid phrases like "your scores indicate" — make it feel like genuine insight
- Maximum 280 words total
- Do not include headers like "Paragraph 1" in your output`;
}

/**
 * Calls the OpenAI (or OpenAI-compatible, e.g. Ollama) Chat Completions API
 * with the assessment prompt. Falls back to a deterministic mock explanation
 * if neither OPENAI_API_KEY nor OPENAI_BASE_URL is configured.
 */
async function generateExplanation(data) {
  if (!isLlmConfigured()) {
    return generateMockExplanation(data.pattern, data.rankedPatterns);
  }

  const prompt = buildPrompt(data);

  const response = await getClient().chat.completions.create({
    model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 450,
    temperature: 0.72,
  });

  return response.choices[0].message.content.trim();
}

function generateMockExplanation(pattern, rankedPatterns) {
  const second = rankedPatterns[1];
  return `[Mock explanation — add OPENAI_API_KEY (or OPENAI_BASE_URL for a local/Ollama model) to backend/.env to enable real AI responses]\n\nYour responses paint a clear picture of someone who naturally operates as ${pattern.name}. The consistency in how you answered questions about decision-making and problem-solving shows that this isn't a surface preference — it's a core cognitive orientation that shapes how you process the world around you.\n\nAs ${pattern.name}, you bring real strengths in ${pattern.description.toLowerCase()} ${second ? `Your secondary alignment with the ${second.name} profile (${second.score.toFixed(1)} pts) adds a complementary dimension, suggesting you're not one-dimensional in your approach.` : ''}\n\nA practical way to leverage this today: deliberately seek out projects that reward your dominant style. When you notice tasks that feel draining or misaligned, ask yourself how you could reframe the work to engage your natural ${pattern.category.toLowerCase()} strengths first.`;
}

module.exports = { generateExplanation, buildPrompt };
