import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import { WebSocketServer } from 'ws';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
const supabaseKey = process.env.SUPABASE_ANON_KEY || 'your-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

const app = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

const SUPPORTED_CHAINS = ['bsc', 'base', 'solana'];
const GECKO_API_BASE = 'https://api.geckoterminal.com/api/v2';

const pairs = new Map();
const orders = new Map();
let orderbookCache = {};

// Rate limiting configuration
const RATE_LIMIT_CONFIG = {
  MIN_DELAY_MS: parseInt(process.env.MIN_REQUEST_DELAY_MS) || 20000,  // 20 seconds between requests
  CHAIN_FETCH_DELAY_MS: parseInt(process.env.CHAIN_FETCH_DELAY_MS) || 120000, // 2 minutes between chain fetches
  MAX_BATCH_SIZE: parseInt(process.env.MAX_BATCH_SIZE) || 2,          // Max parallel requests
  RETRY_COUNT: parseInt(process.env.RETRY_COUNT) || 5,                // Number of retries
  RETRY_BACKOFF_MS: parseInt(process.env.RETRY_BACKOFF_MS) || 5000,   // Base backoff for server errors
};

const requestQueue = [];
let isProcessing = false;

const fetchWithRetry = async (url, retries = RATE_LIMIT_CONFIG.RETRY_COUNT) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, {
        headers: { 
          'Accept': 'application/json',
          'User-Agent': 'CexDex-Bot/1.0'
        }
      });
      if (!response.ok) {
        if (response.status === 429) {
          const retryAfter = parseInt(response.headers.get('Retry-After') || '30', 10);
          const waitMs = (Number.isNaN(retryAfter) ? 30000 : retryAfter * 1000) * (i + 1);
          console.log(`Rate limited! Waiting ${waitMs / 1000}s before retry ${i + 1}/${retries}...`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }
        if (response.status >= 500 && i < retries - 1) {
          const waitMs = RATE_LIMIT_CONFIG.RETRY_BACKOFF_MS * (i + 1);
          console.log(`Server error, waiting ${waitMs / 1000}s before retry...`);
          await new Promise(r => setTimeout(r, waitMs));
          continue;
        }
        throw new Error(`HTTP ${response.status}`);
      }
      return await response.json();
    } catch (err) {
      if (i === retries - 1) throw err;
      const waitMs = 1000 * Math.pow(2, i);
      await new Promise(r => setTimeout(r, waitMs));
    }
  }
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
};

const enqueueRequest = async (fn) => {
  return new Promise((resolve, reject) => {
    requestQueue.push({ fn, resolve, reject });
    if (!isProcessing) {
      processQueue();
    }
  });
};

const processQueue = async () => {
  if (isProcessing || requestQueue.length === 0) return;
  isProcessing = true;

  while (requestQueue.length > 0) {
    const batch = requestQueue.splice(0, RATE_LIMIT_CONFIG.MAX_BATCH_SIZE);
    
    // Process batch
    const results = await Promise.allSettled(
      batch.map(item => item.fn().then(item.resolve).catch(item.reject))
    );

    // Wait before next batch
    if (requestQueue.length > 0) {
      console.log(`Waiting ${RATE_LIMIT_CONFIG.MIN_DELAY_MS / 1000}s before next batch (${requestQueue.length} remaining)...`);
      await new Promise(r => setTimeout(r, RATE_LIMIT_CONFIG.MIN_DELAY_MS));
    }
  }

  isProcessing = false;
};

const getPoolInfo = async (network, poolAddress) => {
  const url = `${GECKO_API_BASE}/networks/${network}/pools/${poolAddress}/info?include=pool`;
  return fetchWithRetry(url);
};

const getTrendingPools = async (network, page = 1, duration = '6h') => {
  const url = `${GECKO_API_BASE}/networks/${network}/trending_pools?include=base_token,quote_token&include_gt_community_data=false&page=${page}&duration=${duration}`;
  return fetchWithRetry(url);
};

const getPoolDetails = async (network, poolAddresses) => {
  const addressesParam = poolAddresses.join(',');
  const url = `${GECKO_API_BASE}/networks/${network}/pools/multi/${addressesParam}?include=base_token,quote_token&include_volume_breakdown=false&include_composition=false`;
  return fetchWithRetry(url);
};

const findTokenInIncluded = (included, tokenId) => {
  if (!included || !tokenId) return null;
  return included.find(t => t.id === tokenId);
};

const isSolanaNetwork = (network) => network === 'solana';
const normalizeAddress = (address, network) => {
  if (!address) return '';
  return isSolanaNetwork(network) ? address : address.toLowerCase();
};

const extractTokenMetadata = (tokenData, network) => {
  if (!tokenData || !tokenData.attributes) return null;
  const attrs = tokenData.attributes;
  
  // Extract nested image object properly
  let imageThumb = '';
  let imageSmall = '';
  let imageLarge = '';
  if (attrs.image && typeof attrs.image === 'object') {
    imageThumb = attrs.image.thumb || '';
    imageSmall = attrs.image.small || '';
    imageLarge = attrs.image.large || '';
  }
  
  // Extract websites array properly
  let websites = [];
  if (Array.isArray(attrs.websites)) {
    websites = attrs.websites.filter(w => typeof w === 'string' && w);
  }
  
  // Extract categories - could be in categories or gt_category_ids
  let categories = [];
  if (Array.isArray(attrs.categories)) {
    categories = attrs.categories.filter(c => typeof c === 'string' && c);
  } else if (Array.isArray(attrs.gt_category_ids)) {
    categories = attrs.gt_category_ids.filter(c => typeof c === 'string' && c);
  }
  
  return {
    address: normalizeAddress(attrs.address || '', network),
    name: attrs.name || '',
    symbol: attrs.symbol || '',
    decimals: parseInt(attrs.decimals, 10) || 18,
    description: attrs.description || '',
    image_url: attrs.image_url || '',
    image_thumb: imageThumb,
    image_small: imageSmall,
    image_large: imageLarge,
    websites: websites,
    twitter_handle: attrs.twitter_handle || '',
    telegram_handle: attrs.telegram_handle || '',
    discord_url: attrs.discord_url || '',
    categories: categories,
    gt_score: parseFloat(attrs.gt_score) || 0,
    gt_verified: attrs.gt_verified === true,
    coingecko_id: attrs.coingecko_coin_id || ''
  };
};

const getTokenAddress = (tokenData, network) => {
  return normalizeAddress(tokenData?.attributes?.address || '', network);
};

const mapPoolInfoTokens = (poolInfoData, baseAddress, quoteAddress, network) => {
  const result = { base: null, quote: null };
  if (!poolInfoData?.data || !Array.isArray(poolInfoData.data)) return result;

  for (const item of poolInfoData.data) {
    if (!item || item.type !== 'token' || !item.attributes) continue;
    const address = getTokenAddress(item, network);
    const tokenMetadata = extractTokenMetadata(item, network);
    if (!address || !tokenMetadata) continue;

    if (address === baseAddress) {
      result.base = tokenMetadata;
    } else if (address === quoteAddress) {
      result.quote = tokenMetadata;
    }
  }

  return result;
};

const syncTrendingPairs = async () => {
  console.log('Starting GeckoTerminal sync...');
  let totalSynced = 0;

  for (const network of SUPPORTED_CHAINS) {
    try {
      console.log(`Fetching trending pools from ${network}...`);
      const trendingData = await getTrendingPools(network, 1, '6h');
      
      if (!trendingData.data || trendingData.data.length === 0) {
        console.log(`No trending pools found for ${network}`);
        continue;
      }

      const poolAddresses = trendingData.data
        .map(pool => normalizeAddress(pool.attributes.address, network))
        .slice(0, 20);
      
      console.log(`Fetching detailed info for ${poolAddresses.length} pools on ${network}...`);
      const multiPoolData = await getPoolDetails(network, poolAddresses);
      
      if (!multiPoolData.data) continue;

      const included = multiPoolData.included || [];

      for (const pool of multiPoolData.data) {
        const attrs = pool.attributes;
        const relationships = pool.relationships;
        
        const baseTokenId = relationships?.base_token?.data?.id;
        const quoteTokenId = relationships?.quote_token?.data?.id;
        const dexId = relationships?.dex?.data?.id;

        const baseTokenData = findTokenInIncluded(included, baseTokenId);
        const quoteTokenData = findTokenInIncluded(included, quoteTokenId);

        const normalizedPairAddress = normalizeAddress(attrs.address, network);
        const pairId = `${network}_${normalizedPairAddress}`;
        const baseSymbol = baseTokenData?.attributes?.symbol || attrs.name?.split('/')[0]?.trim() || '???';
        const quoteSymbol = quoteTokenData?.attributes?.symbol || attrs.name?.split('/')[1]?.replace(/\s*\d+(\.\d+)?%?$/, '')?.trim() || '???';

        // Skip BNB pairs on BSC and specific quote tokens on Base
        if ((network === 'bsc' && quoteSymbol.toUpperCase() === 'BNB') ||
            (network === 'base' && ['ETH', 'MUSD', 'FIETH'].includes(quoteSymbol.toUpperCase()))) {
          console.log(`Skipping ${network} pair ${pairId} with quote token ${quoteSymbol}`);
          continue;
        }

        // Skip if pair already exists (prevent duplicates)
        if (pairs.has(pairId)) {
          console.log(`Skipping duplicate pair ${pairId}`);
          continue;
        }

        // Use included token metadata from the multi-pool response first
        let baseTokenInfo = baseTokenData ? extractTokenMetadata(baseTokenData, network) : null;
        let quoteTokenInfo = quoteTokenData ? extractTokenMetadata(quoteTokenData, network) : null;
        let baseTokenDecimals = baseTokenInfo?.decimals || 18;
        let quoteTokenDecimals = quoteTokenInfo?.decimals || 18;
        const baseTokenAddress = baseTokenInfo?.address || normalizeAddress(baseTokenData?.attributes?.address, network) || '';
        const quoteTokenAddress = quoteTokenInfo?.address || normalizeAddress(quoteTokenData?.attributes?.address, network) || '';

        const needsInfoFetch = baseTokenAddress && quoteTokenAddress;
        if (needsInfoFetch) {
          try {
            // Use rate-limited queue for pool info requests
            const poolInfoData = await enqueueRequest(() => 
              getPoolInfo(network, normalizedPairAddress)
            );
            const mapped = mapPoolInfoTokens(poolInfoData, baseTokenAddress, quoteTokenAddress, network);
            if (mapped.base) {
              baseTokenInfo = mapped.base;
              baseTokenDecimals = mapped.base.decimals || baseTokenDecimals;
            }
            if (mapped.quote) {
              quoteTokenInfo = mapped.quote;
              quoteTokenDecimals = mapped.quote.decimals || quoteTokenDecimals;
            }
            console.log(`✓ Fetched pool info for ${pairId}: base decimals=${baseTokenDecimals}, quote decimals=${quoteTokenDecimals}`);
          } catch (err) {
            console.log(`⚠ Could not fetch pool info for ${pairId}, using defaults: ${err.message}`);
          }
        }

        if (!baseTokenInfo && baseTokenData) {
          baseTokenInfo = extractTokenMetadata(baseTokenData, network) || {
            address: normalizeAddress(baseTokenData?.attributes?.address, network) || '',
            name: baseTokenData?.attributes?.name || baseSymbol,
            symbol: baseSymbol,
            decimals: baseTokenDecimals
          };
        }
        if (!quoteTokenInfo && quoteTokenData) {
          quoteTokenInfo = extractTokenMetadata(quoteTokenData, network) || {
            address: normalizeAddress(quoteTokenData?.attributes?.address, network) || '',
            name: quoteTokenData?.attributes?.name || quoteSymbol,
            symbol: quoteSymbol,
            decimals: quoteTokenDecimals
          };
        }

        if (!baseTokenInfo) {
          baseTokenInfo = {
            address: '',
            name: baseSymbol,
            symbol: baseSymbol,
            decimals: baseTokenDecimals
          };
        }
        if (!quoteTokenInfo) {
          quoteTokenInfo = {
            address: '',
            name: quoteSymbol,
            symbol: quoteSymbol,
            decimals: quoteTokenDecimals
          };
        }

         pairs.set(pairId, {
          id: pairId,
          network,
          pair_address: normalizedPairAddress,
          dex_name: attrs.pool_name?.includes('PancakeSwap') ? 'PancakeSwap' : 
                   attrs.pool_name?.includes('Uniswap') ? 'Uniswap' : 
                   attrs.pool_name?.includes('Aero') ? 'Aero' : 
                   attrs.pool_name?.includes('Raydium') ? 'Raydium' : 
                   attrs.pool_name?.split(' ')[0] || 'DEX',
          base_token: baseTokenInfo ? {
            address: baseTokenInfo.address,
            name: baseTokenInfo.name,
            symbol: baseTokenInfo.symbol,
            logo: baseTokenInfo.image_url || baseTokenInfo.image_large || '',
            decimals: baseTokenInfo.decimals
          } : null,
          quote_token: quoteTokenInfo ? {
            address: quoteTokenInfo.address,
            name: quoteTokenInfo.name,
            symbol: quoteTokenInfo.symbol,
            logo: quoteTokenInfo.image_url || quoteTokenInfo.image_large || '',
            decimals: quoteTokenInfo.decimals
          } : null,
          base_symbol: baseTokenInfo?.symbol || '',
          quote_symbol: quoteTokenInfo?.symbol || '',
          dex: attrs.pool_name?.split(' ')[0] || 'DEX',
          pool_address: normalizedPairAddress,
          base_token_decimals: baseTokenDecimals,
          quote_token_decimals: quoteTokenDecimals,
          base_token_info: baseTokenInfo,
          quote_token_info: quoteTokenInfo,
          pool_name: attrs.pool_name || attrs.name,
          market_cap_usd: attrs.market_cap_usd || 0,
          market_cap: Math.floor(attrs.market_cap_usd || 0),
          created_at: attrs.pool_created_at,
          indexed_at: new Date().toISOString()
        });
        totalSynced++;
      }

      console.log(`Synced ${multiPoolData.data.length} pairs from ${network}`);

      if (network !== SUPPORTED_CHAINS[SUPPORTED_CHAINS.length - 1]) {
        console.log(`Waiting ${RATE_LIMIT_CONFIG.CHAIN_FETCH_DELAY_MS / 1000}s before fetching next chain...`);
        await new Promise(r => setTimeout(r, RATE_LIMIT_CONFIG.CHAIN_FETCH_DELAY_MS));
      }

    } catch (error) {
      console.error(`Error syncing ${network}:`, error.message);
    }
  }

  console.log(`Total synced: ${totalSynced} pairs`);
  return totalSynced;
};

// Save pairs to Supabase
// NOTE: Ensure your Supabase 'pairs' table has a UNIQUE constraint on 'id' column
// Otherwise duplicates will be created. If you have duplicates, run:
// DELETE FROM pairs WHERE id NOT IN (SELECT MIN(id) FROM pairs GROUP BY id)
const savePairsToSupabase = async (pairsData) => {
  try {
    const { data, error } = await supabase
      .from('pairs')
      .upsert(pairsData, { onConflict: 'id' })
      .select();
    
    if (error) {
      console.error('Supabase save error:', error.message);
      return false;
    }
    console.log(`Saved ${pairsData.length} pairs to Supabase`);
    return true;
  } catch (err) {
    console.error('Supabase error:', err.message);
    return false;
  }
};

// Fetch pairs from Supabase
const fetchPairsFromSupabase = async () => {
  try {
    const { data, error } = await supabase
      .from('pairs')
      .select('*')
      .order('indexed_at', { ascending: false })
      .limit(100);
    
    if (error) {
      console.error('Supabase fetch error:', error.message);
      return null;
    }
    return data || [];
  } catch (err) {
    console.error('Supabase fetch error:', err.message);
    return null;
  }
};

// Initialize: load from Supabase first, then sync
const initializePairs = async () => {
  // Try loading from Supabase first
  const cachedPairs = await fetchPairsFromSupabase();
  if (cachedPairs && cachedPairs.length > 0) {
    cachedPairs.forEach(pair => pairs.set(pair.id, pair));
    console.log(`Loaded ${pairs.size} pairs from Supabase`);
    return;
  }
  
  // If no cached pairs, fetch from GeckoTerminal
  console.log('No cached pairs in Supabase, fetching from GeckoTerminal...');
  const count = await syncTrendingPairs();
  
  // Save to Supabase
  const pairsArray = Array.from(pairs.values());
  await savePairsToSupabase(pairsArray);
  console.log(`Initial sync complete: ${count} pairs loaded`);
};

// Run sync every 15 minutes
cron.schedule('*/15 * * * *', async () => {
  console.log('Running scheduled sync from GeckoTerminal...');
  await syncTrendingPairs();
  
  // Save to Supabase
  const pairsArray = Array.from(pairs.values());
  await savePairsToSupabase(pairsArray);
});

// API Endpoints
const handlePairsList = async (req, res) => {
  const network = req.query.network;

  // Try Supabase first
  let dbPairs = await fetchPairsFromSupabase();
  
  // Filter by network if provided
  if (network && dbPairs && dbPairs.length > 0) {
    dbPairs = dbPairs.filter(p => p.network === network);
  }
  
  if (dbPairs && dbPairs.length > 0) {
    return res.json(dbPairs);
  }
  
  // Fallback to memory - also filter by network
  let memoryPairs = Array.from(pairs.values());
  if (network) {
    memoryPairs = memoryPairs.filter(p => p.network === network);
  }
  res.json(memoryPairs);
};

const handleTrendingPairs = async (req, res) => {
  const network = req.query.network;

  // Try Supabase first
  let dbPairs = await fetchPairsFromSupabase();
  if (network && dbPairs && dbPairs.length > 0) {
    dbPairs = dbPairs.filter(p => p.network === network);
  }
  if (dbPairs && dbPairs.length > 0) {
    const sorted = dbPairs.sort((a, b) => (b.trendingScore || 0) - (a.trendingScore || 0));
    return res.json(sorted);
  }
  
  // Fallback to memory
  let memoryPairs = Array.from(pairs.values());
  if (network) {
    memoryPairs = memoryPairs.filter(p => p.network === network);
  }
  const sorted = memoryPairs.sort((a, b) => (b.trendingScore || 0) - (a.trendingScore || 0));
  res.json(sorted);
};

const handlePairById = (req, res) => {
  const pair = pairs.get(req.params.id);
  if (!pair) {
    return res.status(404).json({ error: 'Pair not found' });
  }
  res.json(pair);
};

const handleSyncPairs = async (req, res) => {
  const count = await syncTrendingPairs();
  res.json({ success: true, count: pairs.size });
};

app.get('/api/pairs', handlePairsList);
app.get('/api/v1/pairs', handlePairsList);

app.get('/api/pairs/trending', handleTrendingPairs);
app.get('/api/v1/pairs/trending', handleTrendingPairs);

app.get('/api/pairs/:id', handlePairById);
app.get('/api/v1/pairs/:id', handlePairById);

app.post('/api/pairs/sync', handleSyncPairs);
app.post('/api/v1/pairs/sync', handleSyncPairs);

// Clean up unwanted pairs (BNB on BSC, ETH on Base)
app.post('/api/pairs/cleanup', async (req, res) => {
  try {
    console.log('Starting cleanup of unwanted pairs...');

    // Delete BNB pairs on BSC
    const { data: bnbPairs, error: bnbError } = await supabase
      .from('pairs')
      .delete()
      .eq('network', 'bsc')
      .eq('quote_symbol', 'BNB')
      .select();

    if (bnbError) {
      console.error('Error deleting BNB pairs:', bnbError.message);
    } else {
      console.log(`Deleted ${bnbPairs?.length || 0} BNB pairs from BSC`);
    }

    // Delete ETH, MUSD, FiETH pairs on Base
    const { data: basePairs, error: baseError } = await supabase
      .from('pairs')
      .delete()
      .eq('network', 'base')
      .in('quote_symbol', ['ETH', 'MUSD', 'FIETH'])
      .select();

    if (baseError) {
      console.error('Error deleting Base pairs:', baseError.message);
    } else {
      console.log(`Deleted ${basePairs?.length || 0} ETH/MUSD/FiETH pairs from Base`);
    }

    const totalDeleted = (bnbPairs?.length || 0) + (basePairs?.length || 0);

    res.json({
      success: true,
      message: 'Cleanup complete',
      deleted_bnb_pairs: bnbPairs?.length || 0,
      deleted_base_pairs: basePairs?.length || 0,
      total_deleted: totalDeleted
    });

  } catch (error) {
    console.error('Cleanup error:', error.message);
    res.status(500).json({ error: 'Cleanup failed', details: error.message });
  }
});

// Clean up duplicate pairs
app.post('/api/pairs/cleanup-duplicates', async (req, res) => {
  try {
    console.log('Starting cleanup of duplicate pairs...');

    // Find duplicate pairs by id
    const { data: allPairs, error: fetchError } = await supabase
      .from('pairs')
      .select('id, created_at')
      .order('id', { ascending: true });

    if (fetchError) {
      return res.status(500).json({ error: 'Failed to fetch pairs', details: fetchError.message });
    }

    // Group by id and find duplicates
    const duplicates = [];
    const seen = new Set();
    
    for (const pair of allPairs) {
      if (seen.has(pair.id)) {
        duplicates.push(pair);
      } else {
        seen.add(pair.id);
      }
    }

    if (duplicates.length === 0) {
      return res.json({ 
        success: true, 
        message: 'No duplicate pairs found', 
        duplicates_removed: 0 
      });
    }

    // Delete duplicates (keep the first occurrence)
    const duplicateIds = duplicates.map(d => d.id);
    const { error: deleteError } = await supabase
      .from('pairs')
      .delete()
      .in('id', duplicateIds);

    if (deleteError) {
      console.error('Error deleting duplicates:', deleteError.message);
      return res.status(500).json({ error: 'Failed to delete duplicates', details: deleteError.message });
    }

    console.log(`Deleted ${duplicates.length} duplicate pairs`);
    res.json({
      success: true,
      message: 'Duplicate cleanup complete',
      duplicates_removed: duplicates.length,
      duplicate_ids: duplicateIds
    });

  } catch (error) {
    console.error('Duplicate cleanup error:', error.message);
    res.status(500).json({ error: 'Duplicate cleanup failed', details: error.message });
  }
});

// Backfill market caps for existing pairs
app.post('/api/pairs/backfill-market-caps', async (req, res) => {
  try {
    console.log('Starting on-demand market cap backfill...');
    
    // Fetch all pairs from Supabase
    const { data: allPairs, error: fetchError } = await supabase
      .from('pairs')
      .select('id, network, pair_address, market_cap_usd');

    if (fetchError) {
      return res.status(500).json({ error: 'Failed to fetch pairs from database', details: fetchError.message });
    }

    // Filter pairs without market cap
    const pairsWithoutMarketCap = allPairs.filter(p => !p.market_cap_usd || p.market_cap_usd === 0);
    
    if (pairsWithoutMarketCap.length === 0) {
      return res.json({ success: true, message: 'All pairs already have market cap data', updated: 0 });
    }

    let updated = 0;
    let failed = 0;

    // Process pairs sequentially with delays to respect rate limits
    for (const pair of pairsWithoutMarketCap) {
      try {
        if (!pair.pair_address) {
          console.warn(`Skipping pair ${pair.id} - no pair address`);
          failed++;
          continue;
        }

        // Fetch pool details from Gecko Terminal
        const url = `${GECKO_API_BASE}/networks/${pair.network}/pools/${pair.pair_address}?include=base_token,quote_token`;
        const poolData = await fetchWithRetry(url);
        
        if (!poolData || !poolData.data) {
          console.warn(`Could not fetch data for pool ${pair.id}`);
          failed++;
          continue;
        }

        const marketCapUSD = poolData.data.attributes?.market_cap_usd || 0;
        const marketCap = Math.floor(marketCapUSD);

        // Update pair in database
        const { error: updateError } = await supabase
          .from('pairs')
          .update({
            market_cap_usd: marketCapUSD,
            market_cap: marketCap,
            updated_at: new Date().toISOString()
          })
          .eq('id', pair.id);

        if (updateError) {
          console.error(`Error updating pair ${pair.id}:`, updateError.message);
          failed++;
          continue;
        }

        console.log(`✓ Updated ${pair.id}: $${marketCapUSD.toLocaleString('en-US', { maximumFractionDigits: 2 })}`);
        updated++;

        // Delay between requests to respect rate limits
        if (updated + failed < pairsWithoutMarketCap.length) {
          const delayMs = RATE_LIMIT_CONFIG.MIN_DELAY_MS;
          console.log(`Waiting ${delayMs / 1000}s before next request... (${updated + failed}/${pairsWithoutMarketCap.length})`);
          await new Promise(r => setTimeout(r, delayMs));
        }

      } catch (error) {
        console.error(`Error processing pair ${pair.id}:`, error.message);
        failed++;
      }
    }

    res.json({ 
      success: true, 
      message: 'Backfill complete',
      updated, 
      failed, 
      total: updated + failed,
      total_pairs: allPairs.length
    });

  } catch (error) {
    console.error('Backfill error:', error.message);
    res.status(500).json({ error: 'Backfill failed', details: error.message });
  }
});

app.get('/api/orderbook/:pairId', (req, res) => {
  const { pairId } = req.params;
  
  if (!orderbookCache[pairId]) {
    orderbookCache[pairId] = generateMockOrderbook(pairId);
  }
  
  res.json(orderbookCache[pairId]);
});

app.post('/api/orders', (req, res) => {
  const { pairId, side, price, amount, userAddress } = req.body;
  
  const order = {
    id: `order-${Date.now()}`,
    pairId,
    side,
    price: parseFloat(price),
    amount: parseFloat(amount),
    userAddress,
    createdAt: new Date().toISOString(),
    status: 'open'
  };
  
  orders.set(order.id, order);
  res.json(order);
});

app.delete('/api/orders/:id', (req, res) => {
  const order = orders.get(req.params.id);
  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }
  
  order.status = 'cancelled';
  orders.set(order.id, order);
  res.json({ success: true });
});

app.get('/api/orders/user/:address', (req, res) => {
  const userOrders = Array.from(orders.values())
    .filter(o => o.userAddress === req.params.address);
  res.json(userOrders);
});

const server = app.listen(PORT, () => {
  console.log(`CexDex server running on port ${PORT}`);
  
  // Initialize pairs on startup
  initializePairs().catch(err => {
    console.error('Failed to initialize pairs on startup:', err.message);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected');
  
  ws.on('message', (message) => {
    const data = JSON.parse(message);
    
    if (data.type === 'subscribe') {
      const pairId = data.pairId || data.pair_id;
      if (!pairId) {
        console.warn('WebSocket subscribe received without pairId', data);
        return;
      }
      console.log('WebSocket subscribe for pair:', pairId);
      if (!orderbookCache[pairId]) {
        orderbookCache[pairId] = generateMockOrderbook(pairId);
      }
      ws.send(JSON.stringify({ type: 'orderbook', data: orderbookCache[pairId] }));
    }
  });
  
  setInterval(() => {
    const pairIds = Object.keys(orderbookCache);
    pairIds.forEach(pairId => {
      orderbookCache[pairId] = generateMockOrderbook(pairId);
    });
    
    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        const pairIds = Object.keys(orderbookCache);
        pairIds.forEach(pairId => {
          client.send(JSON.stringify({ type: 'orderbook', pairId, data: orderbookCache[pairId] }));
        });
      }
    });
  }, 5000);
});

console.log('WebSocket server started');
