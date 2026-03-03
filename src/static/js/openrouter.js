const DEFAULT_MODEL_COST = 0.01;

function _sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function _pollJob(jobId, signal, timeoutMs = 120000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const resp = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`, { cache: 'no-store', signal });
    if (!resp.ok) throw new Error(`Polling failed: HTTP ${resp.status}`);
    const data = await resp.json();
    const job = data.job || {};
    if (['completed', 'failed', 'cancelled'].includes(job.status)) {
      return job;
    }
    const retryAfter = Number(resp.headers.get('Retry-After') || 0);
    await _sleep(Math.max(1000, retryAfter * 1000));
  }
  throw new Error('Generation timed out');
}

window.OpenRouter = {
  MODELS: [],
  MODEL_COSTS: {},
  MODEL_MODALITIES: {},

  async init() {
    if (this.MODELS.length > 0) return this.MODELS;
    try {
      const resp = await fetch('/api/models', { cache: 'no-store' });
      const payload = await resp.json();
      const rawModels = Array.isArray(payload.models) ? payload.models : [];
      if (rawModels.length > 0 && typeof rawModels[0] === 'object') {
        this.MODELS = rawModels.map((m) => ({
          id: String(m.id || m.model || ''),
          label: String(m.label || m.id || m.model || 'Model'),
          cost: Number(m.cost_per_image || m.cost || DEFAULT_MODEL_COST),
          modality: String(m.modality || 'image'),
        })).filter((m) => m.id);
      } else {
        this.MODELS = rawModels.map((id) => ({ id: String(id), label: String(id), cost: DEFAULT_MODEL_COST, modality: 'image' }));
      }
      this.MODELS.forEach((m) => {
        this.MODEL_COSTS[m.id] = Number(m.cost || DEFAULT_MODEL_COST);
        this.MODEL_MODALITIES[m.id] = m.modality || 'image';
      });
      return this.MODELS;
    } catch (err) {
      console.warn('Unable to load models from backend:', err.message);
      this.MODELS = [
        { id: 'openrouter/openai/gpt-5-image', label: 'GPT-5 Image', cost: 0.04, modality: 'both' },
        { id: 'openrouter/google/gemini-2.5-flash-image', label: 'Gemini 2.5 Flash Image', cost: 0.003, modality: 'both' },
      ];
      this.MODELS.forEach((m) => {
        this.MODEL_COSTS[m.id] = m.cost;
        this.MODEL_MODALITIES[m.id] = m.modality;
      });
      return this.MODELS;
    }
  },

  async generateImage(prompt, model, _apiKey, signal, timeoutMs = 120000, options = {}) {
    const requestedVariants = Math.max(1, Number(options.variants || 1));
    const requestedVariant = Math.max(1, Number(options.variant || 1));
    const pickResult = (results) => {
      const rows = Array.isArray(results) ? results : [];
      if (!rows.length) return null;
      const exact = rows.find((row) => Number(row?.variant || row?.variant_id || 0) === requestedVariant);
      return exact || rows[requestedVariant - 1] || rows[0];
    };

    const payload = {
      catalog: options.catalog || 'classics',
      book: Number(options.book_id || options.bookNumber || options.book || 0),
      models: [model],
      variants: requestedVariants,
      prompt_source: options.prompt_source || 'custom',
      prompt,
      cover_source: options.cover_source || 'drive',
      async: true,
    };
    if (options.provider) payload.provider = String(options.provider).trim().toLowerCase();
    if (options.idempotency_key) payload.idempotency_key = String(options.idempotency_key).trim();
    if (options.max_attempts) payload.max_attempts = Math.max(1, Number(options.max_attempts || 1));

    const generateResp = await fetch('/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal,
    });

    if (generateResp.status === 429) {
      const retryAfter = Number(generateResp.headers.get('Retry-After') || 5);
      await _sleep(retryAfter * 1000);
      throw new Error('RATE_LIMITED');
    }

    if (!generateResp.ok) {
      const text = await generateResp.text();
      throw new Error(`Generation request failed: ${generateResp.status} ${text}`);
    }

    const generateData = await generateResp.json();
    const immediate = generateData.job || {};
    if (immediate.status === 'completed' && immediate.result) {
      const result = immediate.result;
      const first = pickResult(result.results);
      return {
        status: 'completed',
        job: immediate,
        result: first,
      };
    }

    const jobId = immediate.id || generateData.job_id;
    if (!jobId) throw new Error('Missing job id');

    const finalJob = await _pollJob(jobId, signal, timeoutMs);
    const finalResults = finalJob.result?.results || [];
    const first = pickResult(finalResults);

    if (finalJob.status !== 'completed') {
      throw new Error(finalJob.error?.message || finalJob.error || 'Generation failed');
    }

    return {
      status: 'completed',
      job: finalJob,
      result: first,
    };
  },
};

window.OpenRouter.init().catch(() => undefined);
