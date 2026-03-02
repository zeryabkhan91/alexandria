const STYLE_POOL = [
  {
    id: 'sevastopol-conflict',
    label: 'Sevastopol / Dramatic Conflict',
    modifier: 'Sevastopol conflict panorama inspired by Vereshchagin and Crimean War field studies, with deep crimson, burnt sienna, cannon-smoke grey, imperial gold, and blood-orange sky saturation.',
  },
  {
    id: 'cossack-epic',
    label: 'Cossack / Epic Journey',
    modifier: 'Cossack epic battle panorama inspired by Repin and Roubaud cavalry motion, with sunburnt ochre, Cossack-red, tarnished silver, deep indigo, and amber horizon light.',
  },
  {
    id: 'golden-atmosphere',
    label: 'Golden Atmosphere',
    modifier: 'Barbizon golden-atmosphere storytelling with liquid gold haze, warm amber highlights, deep forest green depth, dusty rose glow, and muted olive transitions.',
  },
  {
    id: 'venetian-renaissance',
    label: 'Venetian Renaissance',
    modifier: 'Venetian Renaissance opulence in the manner of Titian and Veronese, using venetian red, lapis lazuli blue, cloth-of-gold yellow, alabaster white, and deep bronze values.',
  },
  {
    id: 'dutch-golden-age',
    label: 'Dutch Golden Age',
    modifier: 'Dutch Golden Age interior light and realism inspired by Vermeer and de Hooch, with candlelight amber, slate blue-grey, mahogany brown, cream linen, and Delft blue accents.',
  },
  {
    id: 'dark-romantic-v2',
    label: 'Dark Romantic v2',
    modifier: 'Dark Romantic v2 atmosphere inspired by Friedrich and Dore, with midnight indigo shadows, icy blue-white highlights, charcoal depth, blood-red accents, and candle-amber glow.',
  },
  {
    id: 'pre-raphaelite-v2',
    label: 'Pre-Raphaelite v2',
    modifier: 'Pre-Raphaelite v2 detail inspired by Waterhouse and Rossetti, with ruby reds, emerald greens, sapphire blues, golden highlights, and pearl skin tones.',
  },
  {
    id: 'art-nouveau-v2',
    label: 'Art Nouveau v2',
    modifier: 'Art Nouveau v2 ornamental rhythm inspired by Mucha and Grasset, with sage green, dusty rose, antique gold, deep teal, and warm ivory harmonies.',
  },
  {
    id: 'ukiyo-e-v2',
    label: 'Ukiyo-e v2',
    modifier: 'Ukiyo-e v2 print language inspired by Hokusai and Hiroshige, with deep indigo, vermillion, pale ochre, celadon green, and rice-paper white contrasts.',
  },
  {
    id: 'noir-v2',
    label: 'Noir v2',
    modifier: 'Noir v2 cinematic tension from 1940s film language, with pure black, silver-white, gunmetal, wet-asphalt grey, and a bold accent in crimson, amber, or neon teal.',
  },
  {
    id: 'botanical-v2',
    label: 'Botanical v2',
    modifier: 'Botanical v2 natural-history precision inspired by Merian and Redoute, with leaf green, petal pink, butterfly orange, lichen yellow, and parchment cream layering.',
  },
  {
    id: 'stained-glass-v2',
    label: 'Stained Glass v2',
    modifier: 'Stained-glass v2 Gothic luminosity, with ruby red panes, cobalt blue panes, emerald green panes, amber gold highlights, and amethyst purple glow.',
  },
  {
    id: 'impressionist-v2',
    label: 'Impressionist v2',
    modifier: 'Impressionist v2 broken-color brushwork inspired by Monet and Renoir, with lavender haze, rose-pink light, sky blue passages, warm peach skin light, and chartreuse spark.',
  },
  {
    id: 'expressionist-v2',
    label: 'Expressionist v2',
    modifier: 'Expressionist v2 emotional distortion inspired by Munch and Kirchner, with acid yellow bursts, blood orange fields, electric blue strokes, toxic green contrasts, and burnt magenta accents.',
  },
  {
    id: 'baroque-v2',
    label: 'Baroque v2',
    modifier: 'Baroque v2 theatrical chiaroscuro inspired by Rubens and Velazquez, with crimson silk, liquid gold, ivory highlights, umber shadows, and near-black depth.',
  },
  {
    id: 'watercolour-v2',
    label: 'Watercolour v2',
    modifier: 'Watercolour v2 vintage illustration softness, with cerulean blue washes, sage green passages, warm grey atmosphere, burnt sienna forms, and violet edge tones.',
  },
  {
    id: 'symbolist-v2',
    label: 'Symbolist v2',
    modifier: 'Symbolist v2 dream logic inspired by Moreau and Redon, with deep purple fields, tarnished gold halos, midnight blue depth, absinthe green veils, and iridescent cyan light.',
  },
  {
    id: 'renaissance-fresco',
    label: 'Renaissance Fresco',
    modifier: 'Renaissance fresco monumentality inspired by Botticelli and Raphael, with terracotta, fresco blue, gold leaf highlights, ivory plaster tones, and sage olive transitions.',
  },
  {
    id: 'russian-realist-v2',
    label: 'Russian Realist v2',
    modifier: 'Russian Realist v2 social drama inspired by the Peredvizhniki school, with ochre earth, raw umber shadows, slate grey weather, birch-white highlights, and blood-red accents.',
  },
  {
    id: 'romantic-sublime',
    label: 'Romantic Sublime',
    modifier: 'Romantic sublime scale inspired by Turner and Church, with molten gold atmosphere, storm-purple clouds, electric white highlights, ocean teal depth, and lavender distance.',
  },
];

function shuffle(items) {
  const arr = [...items];
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

window.StyleDiversifier = {
  STYLE_POOL,

  selectDiverseStyles(count = 1) {
    const n = Math.max(0, Number(count || 0));
    if (n === 0) return [];
    const out = [];
    while (out.length < n) {
      const shuffled = shuffle(STYLE_POOL);
      for (const style of shuffled) {
        out.push(style);
        if (out.length >= n) break;
      }
    }
    return out;
  },

  buildDiversifiedPrompt(title, author, style) {
    return [
      `Create a colorful, richly colored circular medallion illustration for the luxury leather-bound edition of "${title}" by ${author}.`,
      'Identify the single most iconic scene or symbolic turning point from this exact story and render it as edge-to-edge narrative artwork.',
      style?.modifier || 'Classical illustration using ruby red, emerald green, cobalt blue, amber gold, and ivory highlights.',
      'Keep one dominant focal subject, layered depth, dense detail, and no empty space or plain background areas.',
      'Composition rules: circular vignette, no text, no letters, no logos, no labels, no ribbon, no plaque, no border, no frame.',
    ].join(' ');
  },
};
