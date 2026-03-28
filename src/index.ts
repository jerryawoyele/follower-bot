import {
  Connection,
  Keypair,
  PublicKey,
  VersionedTransaction,
} from "@solana/web3.js";
import {
  getAssociatedTokenAddressSync,
  TOKEN_PROGRAM_ID,
  TOKEN_2022_PROGRAM_ID,
} from "@solana/spl-token";
import bs58 from "bs58";
import dotenv from "dotenv";
import WebSocket from "ws";
import { CpAmm } from "@meteora-ag/cp-amm-sdk";

dotenv.config();

const SOL_MINT = "So11111111111111111111111111111111111111112";
const ULTRA_BASE = "https://api.jup.ag/ultra/v1";
const HELIUS_BASE = "https://api-mainnet.helius-rpc.com/v0";

type Side = "BUY" | "SELL" | "UNKNOWN";

type BotConfig = {
  rpcUrl: string;
  heliusApiKey: string;
  privateKey: string;
  jupiterApiKey: string;

  // wallet you are copying
  leaderWallet: string;

  // how much SOL your bot uses per copied buy
  buyAmountLamports: bigint;

  // ultra params
  slippageBps?: number;
  priorityFeeLamports?: number;
  broadcastFeeType?: "maxCap" | "exactFee";
  jitoTipLamports?: number;

  // TP/SL params
  tp1Trigger?: number; // e.g., 1.5 = sell when price increases 50%
  tp1SellPercent?: number; // e.g., 50 = sell 50% of position at TP1
  tp2Trigger?: number; // e.g., 3.0 = sell remaining when price triples
  slTrigger?: number; // e.g., 0.5 = sell remaining when price drops 50%
  smartSlCeiling?: number; // e.g., 1.9 = track that price reached this high
  smartSlFloor?: number; // e.g., 1.6 = sell if price drops to this after hitting ceiling
  tokenAccountSlLeader?: string; // Wallet that owns token accounts to monitor for SL
  tokenAccountSlEnabled?: boolean; // Enable/disable token account SL (default: false)
  tokenAccountCheckIntervalMs?: number; // How often to check token account txs (default: 15s)
  priceCheckIntervalMs?: number; // how often to check prices (default: 5s)
  // Pool monitoring params
  poolRpcUrl?: string; // Separate RPC for pool monitoring (avoid rate limits)
  poolCheckIntervalMs?: number; // How often to check pool state (default: 2s)
  profitExitPercent?: number; // Profit % to trigger exit when momentum slowing (default: 20%)
  // Early score engine params
  leaderWallets?: string[]; // High-weight known wallets (score +10 each)
  followerWallets?: string[]; // Lower-weight known wallets (score +4 each)
  insiderWallets?: string[]; // Known insider wallets (score +3 each)
};

type TokenDelta = {
  mint: string;
  owner: string;
  pre: bigint;
  post: bigint;
  delta: bigint;
};

type SwapSignal = {
  signature: string;
  mint: string;
  side: Side;
  tokenDelta: bigint;
  solAmount: number;
  slot: number;
};

type OrderExecuteResult = {
  success: boolean;
  signature?: string;
  requestId?: string;
  status?: string;
  error?: string;
  raw?: any;
};

type JupiterOrderResponse = {
  transaction?: string;
  requestId?: string;
};

type JupiterExecuteResponse = {
  status?: string;
  signature?: string;
  txid?: string;
};

// Early score engine types
type TxSide = "buy" | "sell" | "unknown";

// Token state machine for sophisticated classification
type TokenState = 
  | "DORMANT"           // Too little evidence yet, watching
  | "STAGE1_STRONG"     // Early score strong, good wallet quality, buys chaining
  | "STAGE1_STALLED"    // Activity present but conviction weak, no buy compression
  | "LATE_IGNITION"     // Dormant token suddenly woke up with fresh buys
  | "STAGE2_CONFIRMED"  // Post-Stage-1 continuation proven with momentum
  | "REJECTED";         // Clear weakness, exit tracking

type EarlyTx = {
  signature: string;
  ts: number;              // ms timestamp
  from?: string;
  side: TxSide;
  amount?: number;         // normalized amount
};

type TokenEarlyMetrics = {
  tokenMint: string;
  poolAddress: string;
  startedAt: number;

  txs: EarlyTx[];
  uniqueWallets: Set<string>;
  knownWalletHits: number;
  leaderWalletHits: number;
  followerWalletHits: number;
  
  // Leader detection - track early appearances
  leaderEarlyHits: number; // Leaders seen in first 20 txs
  first20LeaderWallets: Set<string>; // Unique leaders in first 20 txs

  buyCount: number;
  sellCount: number;
  unknownCount: number;

  repeatedWalletCount: number;
  walletSeenCount: Map<string, number>;
  seenSignatures: Set<string>; // Deduplication - signatures already processed
  rawFetchedCount: number; // Total fetched from API
  duplicateCount: number; // Duplicates skipped

  amounts: number[];
  firstSellAtTx?: number;
  firstSellAtMs?: number;

  score: number;
  decision: "good" | "bad" | "pending" | "watchlist" | "none";
  evaluated: boolean;
  
  // State machine for sophisticated classification
  state: TokenState;
  prevKnownHits: number; // Known hits from previous check (for delta)
  knownHitsDelta: number; // Change in known hits recently
  newTxsLastBatch: number; // New txs in most recent fetch
  buyCompression: boolean; // Are buys compressing (consecutive buys >= 2)
  ignitionDetectedAt?: number; // Timestamp when LATE_IGNITION was detected
  
  // Two-stage entry system
  stage: 1 | 2; // Stage 1 = early structure, Stage 2 = momentum confirmation
  stage1Score?: number; // Score from stage 1
  stage1Decision?: "good" | "bad" | "pending" | "watchlist";
  stage2Confirmations: number; // Count of confirmation signals
  priceHigh?: number; // Highest price seen (for breakout detection)
  consecutiveBuySamples: number; // Pool samples with consecutive buy pressure
  walletAcceleration: number; // Rate of new unique wallets
};

// Pool snapshot for monitoring
type PoolSnapshot = {
  tokenReserve: number;
  quoteReserve: number;
  price: number;
  liquidity: number;
  ts: number;
};

// Pool transaction for early score engine
type PoolTx = {
  signature: string;
  timestamp: number;
  wallet?: string;
  side: "buy" | "sell" | "unknown";
  amount: number;
};

// Wallet profile for dynamic classification
type WalletProfile = {
  address: string;
  leaderScore: number;      // +2 for each early appearance (tx <= 15)
  followerScore: number;    // +1 for each mid appearance (tx 15-100)
  earlyAppearances: number; // Count of appearances in first 15 txs
  totalAppearances: number; // Total appearances across all tokens
  tokensSeen: Set<string>;  // Which tokens this wallet appeared in
  lastSeenAt: number;       // Timestamp of last appearance
};

// Pending position - migration detected but not yet bought
type PendingPosition = {
  mint: string;
  poolAddress: string;
  leaderBuySignature: string;
  slot: number;
  prevPoolSnapshot?: PoolSnapshot;
  lastDirection: "BUY" | "SELL" | "NONE";
  lastPriceMove: number;
};

type FollowState = {
  mint: string;
  botAta: string;
  leaderBuySignature: string;
  ourBuySignature?: string;
  openedAtSlot: number;
  tokenAmountBought: bigint; // Remaining tokens to sell (decreases after partial sells)
  originalTokenAmount: bigint; // Original amount bought (never changes, used for price queries)
  buyPriceInSol: number; // SOL amount spent (entry price for TP/SL)
  tp1Hit: boolean; // Whether TP1 has been triggered (for partial sell tracking)
  highestMultiplier: number; // Highest multiplier reached (for smart SL)
  // Token Account SL tracking
  leaderTokenAccount?: string; // Token account address owned by TOKEN_ACCOUNT_LEADER
  lastCheckedSignature?: string; // Last signature checked for token account SL
  buyTxCount: number; // Count of buy txs (2074080 native transfer amount)
  // Pool monitoring state
  poolAddress?: string; // DAMM v2 pool address (extracted from migration tx)
  prevPoolSnapshot?: PoolSnapshot; // Previous pool snapshot for comparison
  flipCount: number; // Direction flip counter
  lastDirection: "BUY" | "SELL" | "NONE"; // Last detected direction
  entryPrice?: number; // Price at entry (from pool)
  highestProfit: number; // Highest profit % reached
  // Enhanced momentum tracking for exits
  momentumScore: number;
  momentumHistory: number[]; // Last N momentum values for slope detection
  momentumIncreasing: boolean;
  consecutiveBuys: number;
  consecutiveSells: number;
  dipsAbsorbed: number;
  lastDipRecovery: number;
  trendBroken: boolean; // Has the trend definitively broken?
  // Breakout protection
  intervalsSinceEntry: number; // How many pool checks since entry
  peakMomentum: number; // Highest momentum seen since entry
  intervalsSincePeakMomentum: number; // How many intervals since peak momentum (for override expiry)
  // Post-entry validation (Stage 1 vs Stage 2)
  entryStage: 1 | 2; // Which stage triggered the buy
  entryTime: number; // Timestamp of entry (ms)
  postEntryValidated: boolean; // Has post-entry structure been validated?
  earlyKillTriggered: boolean; // Was early kill switch activated?
  // Insider movement tracking during position
  insiderBuysSinceEntry: number; // Insider buys after bot entry
  insiderSellsSinceEntry: number; // Insider sells after bot entry
  lastInsiderActivity?: { wallet: string; side: "buy" | "sell"; timestamp: number };
  // Insider momentum tracking (rate of insider buys per interval)
  insiderBuysThisInterval: number; // Insider buys in current interval
  insiderBuysPrevInterval: number; // Insider buys in previous interval
  peakInsiderBuyRate: number; // Highest insider buy rate seen
};

// Helius API types
type HeliusTokenBalance = {
  accountIndex: number;
  mint: string;
  owner?: string;
  uiTokenAmount: {
    amount: string;
    decimals: number;
    uiAmount: number | null;
    uiAmountString: string;
  };
};

type HeliusInstruction = {
  programId: string;
  accounts: string[];
  data: string;
  parsed?: any;
};

type HeliusInnerInstruction = {
  index: number;
  instructions: HeliusInstruction[];
};

type HeliusTokenTransfer = {
  fromTokenAccount?: string;
  toTokenAccount?: string;
  fromUserAccount?: string;
  toUserAccount?: string;
  tokenAmount: number;
  mint: string;
  tokenStandard: string;
};

type HeliusNativeTransfer = {
  fromUserAccount?: string;
  toUserAccount?: string;
  amount: number;
};

type HeliusTransaction = {
  signature: string;
  slot: number;
  timestamp: number;
  type: string;
  source?: string;
  nativeTransfers?: HeliusNativeTransfer[];
  tokenTransfers?: HeliusTokenTransfer[];
  accountData?: any;
  description?: string;
  fee: number;
  feePayer: string;
  transactionError?: any;
  instructions: HeliusInstruction[];
  innerInstructions?: HeliusInnerInstruction[];
  preTokenBalances?: HeliusTokenBalance[];
  postTokenBalances?: HeliusTokenBalance[];
};

export class JupiterUltraTrader {
  private connection: Connection;
  private owner: Keypair;
  private jupiterApiKey: string;
  private defaultSlippageBps: number;
  private defaultPriorityFeeLamports: number;
  private broadcastFeeType?: "maxCap" | "exactFee";
  private jitoTipLamports?: number;

  constructor(config: BotConfig) {
    this.connection = new Connection(config.rpcUrl, "confirmed");
    this.owner = Keypair.fromSecretKey(bs58.decode(config.privateKey));
    this.jupiterApiKey = config.jupiterApiKey;
    this.defaultSlippageBps = config.slippageBps ?? 1000;
    this.defaultPriorityFeeLamports = config.priorityFeeLamports ?? 300000;
    this.broadcastFeeType = config.broadcastFeeType;
    this.jitoTipLamports = config.jitoTipLamports;
  }

  get ownerPubkey(): PublicKey {
    return this.owner.publicKey;
  }

  async buyToken(params: {
    outputMint: string;
    amountLamports: bigint;
  }): Promise<OrderExecuteResult> {
    return this.orderAndExecute({
      inputMint: SOL_MINT,
      outputMint: params.outputMint,
      amount: params.amountLamports.toString(),
    });
  }

  async sellToken100Percent(params: {
    inputMint: string;
  }): Promise<OrderExecuteResult> {
    // Try both TOKEN_PROGRAM_ID and TOKEN_2022_PROGRAM_ID
    const ataStandard = this.findAtaForMint(params.inputMint, TOKEN_PROGRAM_ID);
    const ata2022 = this.findAtaForMint(params.inputMint, TOKEN_2022_PROGRAM_ID);

    let amount: string | null = null;

    // Try standard ATA first
    try {
      const balance = await this.connection.getTokenAccountBalance(ataStandard, "confirmed");
      amount = balance?.value?.amount ?? null;
      if (amount && amount !== "0") {
        return this.orderAndExecute({
          inputMint: params.inputMint,
          outputMint: SOL_MINT,
          amount,
        });
      }
    } catch {}

    // Try Token-2022 ATA
    try {
      const balance = await this.connection.getTokenAccountBalance(ata2022, "confirmed");
      amount = balance?.value?.amount ?? null;
      if (amount && amount !== "0") {
        return this.orderAndExecute({
          inputMint: params.inputMint,
          outputMint: SOL_MINT,
          amount,
        });
      }
    } catch {}

    return { success: false, error: "No token account or balance found" };
  }

  async sellTokenAmount(params: {
    inputMint: string;
    amount: bigint;
  }): Promise<OrderExecuteResult> {
    if (params.amount <= 0n) {
      return { success: false, error: "Amount must be greater than 0" };
    }

    return this.orderAndExecute({
      inputMint: params.inputMint,
      outputMint: SOL_MINT,
      amount: params.amount.toString(),
    });
  }

  getAtaForMint(mint: string): string {
    return this.findAtaForMint(mint, TOKEN_PROGRAM_ID).toBase58();
  }

  private findAtaForMint(mint: string, programId: typeof TOKEN_PROGRAM_ID | typeof TOKEN_2022_PROGRAM_ID): PublicKey {
    return getAssociatedTokenAddressSync(
      new PublicKey(mint),
      this.owner.publicKey,
      false,
      programId
    );
  }

  private async orderAndExecute(params: {
    inputMint: string;
    outputMint: string;
    amount: string;
  }): Promise<OrderExecuteResult> {
    const qs = new URLSearchParams({
      inputMint: params.inputMint,
      outputMint: params.outputMint,
      amount: params.amount,
      taker: this.owner.publicKey.toBase58(),
      slippageBps: String(this.defaultSlippageBps),
      priorityFeeLamports: String(this.defaultPriorityFeeLamports),
    });

    if (this.broadcastFeeType) {
      qs.set("broadcastFeeType", this.broadcastFeeType);
    }
    // Jupiter requires jitoTipLamports >= 1000 if provided
    if (typeof this.jitoTipLamports === "number" && this.jitoTipLamports >= 1000) {
      qs.set("jitoTipLamports", String(this.jitoTipLamports));
    }

    // Retry delays for quote failures (fresh tokens may need time)
    const retryDelays = [0, 300, 700, 1500];
    let lastOrderError: string | null = null;

    for (let i = 0; i < retryDelays.length; i++) {
      if (retryDelays[i] > 0) {
        await new Promise((r) => setTimeout(r, retryDelays[i]));
      }

      const orderResp = await fetch(`${ULTRA_BASE}/order?${qs.toString()}`, {
        method: "GET",
        headers: {
          "x-api-key": this.jupiterApiKey,
          Accept: "application/json",
        },
      });

      const orderJson = (await orderResp.json()) as JupiterOrderResponse;
      
      if (!orderResp.ok) {
        lastOrderError = `Order HTTP ${orderResp.status}: ${JSON.stringify(orderJson)}`;
        
        // Only retry on quote failures (400 with "Failed to get quotes")
        const isQuoteError = orderResp.status === 400 && 
          JSON.stringify(orderJson).includes("Failed to get quotes");
        
        if (isQuoteError && i < retryDelays.length - 1) {
          console.log(`[Bot] Quote failed, retrying in ${retryDelays[i + 1]}ms...`);
          continue;
        }
        
        return {
          success: false,
          error: lastOrderError,
          raw: orderJson,
        };
      }

      if (!orderJson?.transaction || !orderJson?.requestId) {
        return {
          success: false,
          error: `Order missing transaction/requestId`,
          raw: orderJson,
        };
      }

      const tx = VersionedTransaction.deserialize(
        Buffer.from(orderJson.transaction, "base64")
      );
      tx.sign([this.owner]);

      const executeResp = await fetch(`${ULTRA_BASE}/execute`, {
        method: "POST",
        headers: {
          "x-api-key": this.jupiterApiKey,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          signedTransaction: Buffer.from(tx.serialize()).toString("base64"),
          requestId: orderJson.requestId,
        }),
      });

      const executeJson = (await executeResp.json()) as JupiterExecuteResponse;
      if (!executeResp.ok) {
        return {
          success: false,
          requestId: orderJson.requestId,
          error: `Execute HTTP ${executeResp.status}: ${JSON.stringify(executeJson)}`,
          raw: { orderJson, executeJson },
        };
      }

      return {
        success: executeJson?.status === "Success",
        signature: executeJson?.signature ?? executeJson?.txid,
        requestId: orderJson.requestId,
        status: executeJson?.status,
        error:
          executeJson?.status === "Success"
            ? undefined
            : `Non-success status: ${JSON.stringify(executeJson)}`,
        raw: { orderJson, executeJson },
      };
    }

    // All retries exhausted
    return {
      success: false,
      error: lastOrderError ?? "All quote retries failed",
    };
  }
}

export class MeteoraDammV2CopyBot {
  private readonly config: BotConfig;

  private readonly trader: JupiterUltraTrader;
  private readonly leader: string;
  private readonly heliusApiKey: string;
  private readonly wsUrl: string;
  private readonly cpAmm!: CpAmm; // Meteora DAMM v2 SDK (initialized in constructor)

  private ws: WebSocket | null = null;
  private readonly processedSignatures = new Set<string>();
  private readonly openPositions = new Map<string, FollowState>();
  private readonly pendingPositions = new Map<string, PendingPosition>(); // Pre-buy state machine
  private readonly attemptedBuys = new Set<string>();
  private readonly soldTokens = new Set<string>();
  private readonly mintToPoolMap = new Map<string, string>(); // mint -> pool address
  
  // Early score engine tracking
  private readonly earlyMetricsMap = new Map<string, TokenEarlyMetrics>(); // mint -> metrics
  private readonly leaderWalletSet!: Set<string>;
  private readonly followerWalletSet!: Set<string>;
  private readonly insiderWalletSet!: Set<string>;
  private readonly knownWalletSet!: Set<string>;
  
  // Dynamic wallet classification
  private readonly walletProfiles = new Map<string, WalletProfile>(); // address -> profile
  private readonly tokenAccountCache = new Map<string, string>(); // "mint:owner" -> tokenAccount
  
  private started = false;
  private shouldStop = false;
  private reconnectAttempts = 0;
  private pingInterval: ReturnType<typeof setInterval> | null = null;
  private livenessCheckInterval: ReturnType<typeof setInterval> | null = null;
  private subscriptionId: number | null = null;
  private lastPongTime: number = 0;
  private lastActivityTime: number = 0;

  // Micro-batching for transaction fetches
  private readonly pendingSignatures = new Set<string>();
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private batchInFlight = false;
  private lastBatchSentAt = 0;
  private readonly BATCH_WINDOW_MS = 100;
  private readonly MAX_BATCH_SIZE = 25;
  private readonly MIN_BATCH_GAP_MS = 200;

  // TP/SL price monitoring
  private priceCheckInterval: ReturnType<typeof setInterval> | null = null;
  private tokenAccountCheckInterval: ReturnType<typeof setInterval> | null = null;
  private poolCheckInterval: ReturnType<typeof setInterval> | null = null;

  constructor(config: BotConfig) {
    this.config = config;
    this.trader = new JupiterUltraTrader(config);
    this.leader = config.leaderWallet;
    this.heliusApiKey = config.heliusApiKey;
    this.wsUrl = `wss://mainnet.helius-rpc.com/?api-key=${config.heliusApiKey}`;
    
    // Initialize CpAmm with pool RPC URL (separate from main RPC to avoid rate limits)
    const poolRpcUrl = config.poolRpcUrl ?? config.rpcUrl;
    const connection = new Connection(poolRpcUrl, "processed");
    this.cpAmm = new CpAmm(connection);
    
    // Initialize known wallet sets for early score engine
    this.leaderWalletSet = new Set(config.leaderWallets ?? []);
    this.followerWalletSet = new Set(config.followerWallets ?? []);
    this.insiderWalletSet = new Set(config.insiderWallets ?? []);
    this.knownWalletSet = new Set([...this.leaderWalletSet, ...this.followerWalletSet, ...this.insiderWalletSet]);
    
    console.log(`[Bot] Loaded ${this.leaderWalletSet.size} LEADER, ${this.followerWalletSet.size} FOLLOWER, ${this.insiderWalletSet.size} INSIDER wallets`);
  }

  // ===== EARLY SCORE ENGINE HELPER FUNCTIONS =====
  
  private createEarlyMetrics(tokenMint: string, poolAddress: string): TokenEarlyMetrics {
    return {
      tokenMint,
      poolAddress,
      startedAt: Date.now(),
      txs: [],
      uniqueWallets: new Set(),
      knownWalletHits: 0,
      leaderWalletHits: 0,
      followerWalletHits: 0,
      leaderEarlyHits: 0,
      first20LeaderWallets: new Set(),
      buyCount: 0,
      sellCount: 0,
      unknownCount: 0,
      repeatedWalletCount: 0,
      walletSeenCount: new Map(),
      seenSignatures: new Set(),
      rawFetchedCount: 0,
      duplicateCount: 0,
      amounts: [],
      score: 0,
      decision: "none",
      evaluated: false,
      // State machine
      state: "DORMANT",
      prevKnownHits: 0,
      knownHitsDelta: 0,
      newTxsLastBatch: 0,
      buyCompression: false,
      ignitionDetectedAt: undefined,
      // Two-stage entry
      stage: 1,
      stage2Confirmations: 0,
      consecutiveBuySamples: 0,
      walletAcceleration: 0,
    };
  }

  // ===== DYNAMIC WALLET CLASSIFICATION =====
  
  // Learn wallets from token mint by querying first 10 txs
  // Look for nativeTransfers with amount=2074080, extract token account from accountData[2]
  private async learnWalletsFromMint(mint: string, tokenAccountLeader: string): Promise<string | null> {
    // Check if we already learned from this mint
    const learnedKey = `learned:${mint}`;
    if (this.tokenAccountCache.has(learnedKey)) {
      return this.tokenAccountCache.get(learnedKey) ?? null;
    }
    
    try {
      // Query token mint directly for first 10 txs (ascending order = oldest first)
      const url = `${HELIUS_BASE}/addresses/${mint}/transactions?api-key=${this.heliusApiKey}&limit=10&sort-order=asc`;
      const resp = await fetch(url);
      if (!resp.ok) return null;
      
      const txs = await resp.json() as any[];
      let tokenAccount: string | null = null;
      
      for (let txIndex = 0; txIndex < txs.length; txIndex++) {
        const tx = txs[txIndex];
        
        // Look for nativeTransfers with amount 2074080 (buy signal)
        if (tx.nativeTransfers && tx.nativeTransfers.length > 0) {
          const transfer = tx.nativeTransfers[0];
          
          if (transfer.amount === 2074080) {
            // Get token account from accountData[2].account
            if (tx.accountData && tx.accountData.length >= 3) {
              tokenAccount = tx.accountData[2].account;
              
              // Get the wallet that received SOL (toUserAccount) - this is the buyer
              const buyerWallet = transfer.toUserAccount;
              
              if (buyerWallet) {
                // Update wallet profile based on tx position
                this.updateWalletProfile(buyerWallet, txIndex, mint);
                
                if (tokenAccount) {
                  console.log(`[Wallet] 🟨 Found buyer: ${buyerWallet.slice(0, 8)}... (tx #${txIndex}, tokenAccount: ${tokenAccount.slice(0, 8)}...)`);
                }
              }
              
              // Cache the token account for this mint
              if (tokenAccount) {
                const cacheKey = `${mint}:${tokenAccountLeader}`;
                this.tokenAccountCache.set(cacheKey, tokenAccount);
                this.tokenAccountCache.set(learnedKey, tokenAccount);
              }
            }
          }
        }
      }
      
      return tokenAccount;
    } catch (err: any) {
      console.error(`[Wallet] Error learning wallets from mint: ${err.message}`);
      return null;
    }
  }

  // Update wallet profile based on tx position
  private updateWalletProfile(wallet: string, txIndex: number, mint: string): void {
    let profile = this.walletProfiles.get(wallet);
    
    if (!profile) {
      profile = {
        address: wallet,
        leaderScore: 0,
        followerScore: 0,
        earlyAppearances: 0,
        totalAppearances: 0,
        tokensSeen: new Set(),
        lastSeenAt: Date.now(),
      };
      this.walletProfiles.set(wallet, profile);
    }
    
    // Only count if this wallet hasn't been seen in this token yet
    if (!profile.tokensSeen.has(mint)) {
      profile.tokensSeen.add(mint);
      profile.totalAppearances++;
      
      // Leader: appears in first 15 txs
      if (txIndex <= 15) {
        profile.leaderScore += 2;
        profile.earlyAppearances++;
        console.log(`[Wallet] 🟨 LEADER score update: ${wallet.slice(0, 8)}... (tx #${txIndex}, leaderScore=${profile.leaderScore})`);
      }
      // Follower: appears in tx 15-100
      else if (txIndex <= 100) {
        profile.followerScore += 1;
        console.log(`[Wallet] 🟦 FOLLOWER score update: ${wallet.slice(0, 8)}... (tx #${txIndex}, followerScore=${profile.followerScore})`);
      }
      
      // Auto-classify based on scores
      this.classifyWallet(wallet, profile);
    }
    
    profile.lastSeenAt = Date.now();
  }

  // Classify wallet as leader/follower based on profile
  private classifyWallet(wallet: string, profile: WalletProfile): void {
    // Leader: score >= 6 AND appeared early in >= 3 tokens
    if (profile.leaderScore >= 6 && profile.earlyAppearances >= 3) {
      if (!this.leaderWalletSet.has(wallet)) {
        this.leaderWalletSet.add(wallet);
        this.knownWalletSet.add(wallet);
        console.log(`[Wallet] 🟨 AUTO-CLASSIFIED AS LEADER: ${wallet} (leaderScore=${profile.leaderScore}, earlyAppearances=${profile.earlyAppearances}, tokensSeen=${profile.tokensSeen.size})`);
      }
    }
    // Follower: score >= 5
    else if (profile.followerScore >= 5) {
      if (!this.followerWalletSet.has(wallet)) {
        this.followerWalletSet.add(wallet);
        this.knownWalletSet.add(wallet);
        console.log(`[Wallet] 🟦 AUTO-CLASSIFIED AS FOLLOWER: ${wallet} (followerScore=${profile.followerScore}, tokensSeen=${profile.tokensSeen.size})`);
      }
    }
  }

  private updateEarlyMetrics(tracker: TokenEarlyMetrics, tx: EarlyTx): void {
    const txIndex = tracker.txs.length; // Get index before push
    tracker.txs.push(tx);

    // Track wallet
    if (tx.from) {
      const prev = tracker.walletSeenCount.get(tx.from) ?? 0;
      tracker.walletSeenCount.set(tx.from, prev + 1);

      if (prev === 0) {
        tracker.uniqueWallets.add(tx.from);
      } else if (prev === 1) {
        tracker.repeatedWalletCount += 1;
      }

      // Check known wallets with weights
      if (this.leaderWalletSet.has(tx.from)) {
        tracker.leaderWalletHits += 1;
        tracker.knownWalletHits += 1;
        
        // Track early leader appearances (first 20 txs)
        if (txIndex < 20 && !tracker.first20LeaderWallets.has(tx.from)) {
          tracker.first20LeaderWallets.add(tx.from);
          tracker.leaderEarlyHits += 1;
        }
      } else if (this.followerWalletSet.has(tx.from)) {
        tracker.followerWalletHits += 1;
        tracker.knownWalletHits += 1;
      } else if (this.insiderWalletSet.has(tx.from)) {
        tracker.knownWalletHits += 1;
        // Insider wallets contribute to knownWalletHits but not leader/follower specific counts
      }
    }

    // Track buy/sell
    if (tx.side === "buy") {
      tracker.buyCount += 1;
    } else if (tx.side === "sell") {
      tracker.sellCount += 1;
      if (tracker.firstSellAtTx === undefined) {
        tracker.firstSellAtTx = tracker.txs.length;
        tracker.firstSellAtMs = tx.ts - tracker.startedAt;
      }
    } else {
      tracker.unknownCount += 1;
    }

    // Track amounts
    if (typeof tx.amount === "number" && Number.isFinite(tx.amount)) {
      tracker.amounts.push(tx.amount);
    }
  }

  private scoreToken(tracker: TokenEarlyMetrics, now: number): number {
    const elapsedMs = now - tracker.startedAt;
    const txCount = tracker.txs.length;
    const classified = tracker.buyCount + tracker.sellCount;

    const txPerSecond = txCount / Math.max(elapsedMs / 1000, 1);
    const uniqueWalletRatio = tracker.uniqueWallets.size / Math.max(txCount, 1);
    const buyRatio = classified > 0 ? tracker.buyCount / classified : 0;
    const knownWalletRatio = tracker.knownWalletHits / Math.max(txCount, 1);
    const repeatedWalletRatio = tracker.repeatedWalletCount / Math.max(txCount, 1);

    let score = 0;

    // 🚨 EARLY LEADER DETECTION - STRONGEST SIGNAL
    // 2+ unique leaders in first 20 txs = huge confidence boost
    if (tracker.leaderEarlyHits >= 2 && txCount <= 30) {
      score += 30; // Huge boost - this is the alpha signal
    } else if (tracker.leaderEarlyHits >= 1 && txCount <= 30) {
      score += 15; // Still strong
    }

    // Speed (tx compression)
    if (txPerSecond >= 3) score += 20;
    else if (txPerSecond >= 1.5) score += 10;

    // Diversity (unique wallets) - CRITICAL METRIC
    if (uniqueWalletRatio >= 0.75) score += 20;
    else if (uniqueWalletRatio >= 0.60) score += 10;
    else score -= 15; // Low diversity = suspicious

    // Buy pressure - handle mixed flow better
    if (buyRatio >= 0.80) score += 20;
    else if (buyRatio >= 0.55) score += 5; // Mixed but still tradable
    else score -= 15;

    // Known swarm presence (weighted)
    // Leaders = +10 each (alpha signal), Followers = +4 each (swarm confirmation), Insiders = +3 each
    const leaderBonus = tracker.leaderWalletHits * 10;
    const followerBonus = tracker.followerWalletHits * 4;
    const insiderHits = tracker.knownWalletHits - tracker.leaderWalletHits - tracker.followerWalletHits;
    const insiderBonus = insiderHits * 3;
    
    if (knownWalletRatio >= 0.5) score += 20;
    else if (knownWalletRatio >= 0.25) score += 10;
    else if (knownWalletRatio >= 0.10) score += 5;
    else score -= 10; // No known wallets = less confidence
    
    score += leaderBonus + followerBonus + insiderBonus;

    // Repetition penalty
    if (repeatedWalletRatio > 0.3) score -= 10;

    // Early sell penalty - only heavy penalty for VERY early sells
    if (tracker.firstSellAtTx !== undefined && tracker.firstSellAtTx <= 20) {
      score -= 25; // Very early sell = bad
    } else if (tracker.firstSellAtTx !== undefined && tracker.firstSellAtTx <= 50) {
      score -= 10; // Early sell but not terrible
    }

    return score;
  }

  private classifyEarlyToken(score: number, tracker: TokenEarlyMetrics): "good" | "bad" | "pending" | "watchlist" {
    const uniqueWalletRatio = tracker.uniqueWallets.size / Math.max(tracker.txs.length, 1);
    const classified = tracker.buyCount + tracker.sellCount;
    const buyRatio = classified > 0 ? tracker.buyCount / classified : 0;
    
    // Good: clean early swarm
    if (score >= 60) return "good";
    
    // Watchlist: insider presence but mixed early tape
    // Keep alive for stage 2 confirmation
    if (score >= 30 && score < 60) {
      if (tracker.knownWalletHits >= 3 && uniqueWalletRatio >= 0.5) {
        return "watchlist"; // Has insider presence, worth watching
      }
      return "pending";
    }
    
    // Bad: weak participation and weak structure
    return "bad";
  }

  // Stage 2 confirmation: check for momentum signals after stage 1
  private checkStage2Confirmation(tracker: TokenEarlyMetrics, snapshot: PoolSnapshot, buyPressure: boolean): boolean {
    if (tracker.stage !== 2) return false;
    
    let confirmed = false;
    
    // Track price high for breakout detection
    if (!tracker.priceHigh || snapshot.price > tracker.priceHigh) {
      tracker.priceHigh = snapshot.price;
      
      // Price breakout = confirmation
      if (tracker.priceHigh > snapshot.price * 1.1) {
        tracker.stage2Confirmations++;
        console.log(`[EarlyScore] 📈 Stage 2 confirmation: Price breakout for ${tracker.tokenMint.slice(0, 8)}... (confirms=${tracker.stage2Confirmations})`);
        confirmed = true;
      }
    }
    
    // Track consecutive buy samples
    if (buyPressure) {
      tracker.consecutiveBuySamples++;
      if (tracker.consecutiveBuySamples >= 3) {
        tracker.stage2Confirmations++;
        console.log(`[EarlyScore] 📈 Stage 2 confirmation: Consecutive buys for ${tracker.tokenMint.slice(0, 8)}... (confirms=${tracker.stage2Confirmations})`);
        confirmed = true;
      }
    } else {
      tracker.consecutiveBuySamples = 0;
    }
    
    // Track wallet acceleration (new unique wallets per tx)
    const prevUniqueCount = tracker.uniqueWallets.size;
    const acceleration = prevUniqueCount > 0 ? (tracker.uniqueWallets.size - prevUniqueCount) / prevUniqueCount : 0;
    if (acceleration > 0.1) {
      tracker.walletAcceleration = acceleration;
      tracker.stage2Confirmations++;
      console.log(`[EarlyScore] 📈 Stage 2 confirmation: Wallet acceleration for ${tracker.tokenMint.slice(0, 8)}... (confirms=${tracker.stage2Confirmations})`);
      confirmed = true;
    }
    
    return confirmed;
  }

  private shouldEvaluateEarly(tracker: TokenEarlyMetrics, now: number): boolean {
    const txCount = tracker.txs.length;
    const elapsedMs = now - tracker.startedAt;

    // Soft classification at 30, real decision at 50, hard reject allowed at 80+
    return (
      txCount === 30 ||
      txCount === 50 ||
      txCount === 80 ||
      txCount === 100 ||
      txCount >= 200 ||
      elapsedMs >= 60_000
    );
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    this.shouldStop = false;

    console.log(`[Bot] Leader wallet: ${this.leader}`);
    console.log(`[Bot] Bot wallet: ${this.trader.ownerPubkey.toBase58()}`);
    console.log(`[Bot] TP1: ${this.config.tp1Trigger ?? 1.5}x (${this.config.tp1SellPercent ?? 50}%) | TP2: ${this.config.tp2Trigger ?? 3.0}x (100%) | SL: ${this.config.slTrigger ?? 0.5}x`);
    console.log(`[Bot] Wallets will be learned from token mints during evaluation`);
    console.log(`[Bot] Connecting to WebSocket...`);

    this.connectWebSocket();
    this.startPriceMonitoring();
    this.startTokenAccountMonitoring();
    this.startPoolMonitoring();
  }

  private connectWebSocket(): void {
    console.log(`[Bot] Opening WebSocket connection...`);
    
    // Reset activity tracking
    this.lastPongTime = Date.now();
    this.lastActivityTime = Date.now();
    
    this.ws = new WebSocket(this.wsUrl);

    this.ws.onopen = () => {
      console.log(`[Bot] WebSocket connected`);
      this.reconnectAttempts = 0;
      this.subscribeToLeaderLogs();
      this.startPingLoop();
      this.startLivenessCheck();
    };

    this.ws.onmessage = (event) => {
      this.lastActivityTime = Date.now();
      this.handleWebSocketMessage(event.data.toString());
    };

    this.ws.onerror = (err) => {
      console.error(`[Bot] WebSocket error:`, err.message);
    };

    this.ws.onclose = (event) => {
      console.log(`[Bot] WebSocket closed: code=${event.code} reason=${event.reason}`);
      this.stopPingLoop();
      this.stopLivenessCheck();
      
      if (!this.shouldStop) {
        this.scheduleReconnect();
      }
    };

    // Handle pong responses
    this.ws.on("pong", () => {
      this.lastPongTime = Date.now();
    });
  }

  private subscribeToLeaderLogs(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.error(`[Bot] Cannot subscribe: WebSocket not open`);
      return;
    }

    const subscribeMsg = {
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [
        {
          mentions: [this.leader],
        },
        {
          commitment: "confirmed",
        },
      ],
    };

    console.log(`[Bot] Subscribing to logs for leader: ${this.leader.slice(0, 8)}...`);
    this.ws.send(JSON.stringify(subscribeMsg));
  }

  private handleWebSocketMessage(data: string): void {
    try {
      const msg = JSON.parse(data);
      
      // Handle subscription confirmation
      if (msg.id === 1 && msg.result !== undefined) {
        this.subscriptionId = msg.result;
        console.log(`[Bot] Subscribed to logs, subscriptionId: ${this.subscriptionId}`);
        return;
      }

      // Handle log notification
      if (msg.method === "logsNotification" && msg.params?.result) {
        const { signature, err } = msg.params.result.value;
        
        if (err) {
          console.log(`[Bot] Skipping failed tx: ${signature.slice(0, 8)}...`);
          return;
        }

        // Skip already processed
        if (this.processedSignatures.has(signature)) {
          return;
        }

        this.enqueueSignature(signature);
      }
    } catch (err: any) {
      console.error(`[Bot] Error parsing WebSocket message:`, err.message);
    }
  }

  private enqueueSignature(signature: string): void {
    this.pendingSignatures.add(signature);

    // Flush immediately if batch is full
    if (this.pendingSignatures.size >= this.MAX_BATCH_SIZE) {
      this.flushBatch();
      return;
    }

    // Start timer if not already running
    if (!this.batchTimer) {
      this.batchTimer = setTimeout(() => {
        this.flushBatch();
      }, this.BATCH_WINDOW_MS);
    }
  }

  private async flushBatch(): Promise<void> {
    if (this.batchInFlight) return;
    if (this.pendingSignatures.size === 0) return;

    this.batchInFlight = true;

    // Clear timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    // Enforce minimum gap between batch requests
    const now = Date.now();
    const wait = Math.max(0, this.MIN_BATCH_GAP_MS - (now - this.lastBatchSentAt));
    if (wait > 0) {
      await new Promise((r) => setTimeout(r, wait));
    }
    this.lastBatchSentAt = Date.now();

    const signatures = Array.from(this.pendingSignatures);
    this.pendingSignatures.clear();

    console.log(`[Bot] Fetching batch of ${signatures.length} transactions...`);

    try {
      const txs = await this.fetchTransactionsBatch(signatures);

      for (const tx of txs) {
        if (!tx) continue;
        this.processedSignatures.add(tx.signature);
        await this.processTransaction(tx);
      }
    } finally {
      this.batchInFlight = false;

      // If more signatures accumulated during fetch, schedule flush with delay
      if (this.pendingSignatures.size > 0) {
        setTimeout(() => this.flushBatch(), this.MIN_BATCH_GAP_MS);
      }
    }
  }

  private async fetchTransactionsBatch(signatures: string[]): Promise<(HeliusTransaction | null)[]> {
    const url = `${HELIUS_BASE}/transactions?api-key=${this.heliusApiKey}`;

    let lastErr: any;

    for (let attempt = 1; attempt <= 4; attempt++) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 15000);

      try {
        const resp = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          body: JSON.stringify({ transactions: signatures }),
          signal: controller.signal,
        });

        clearTimeout(timeout);

        if (resp.status === 429) {
          const backoffMs = Math.min(5000, 500 * Math.pow(2, attempt - 1));
          console.error(`[Bot] Batch fetch hit 429, retrying in ${backoffMs}ms...`);
          await new Promise((r) => setTimeout(r, backoffMs));
          continue;
        }

        if (!resp.ok) {
          console.error(`[Bot] Batch fetch failed: HTTP ${resp.status}`);
          return signatures.map(() => null);
        }

        return (await resp.json()) as (HeliusTransaction | null)[];
      } catch (err: any) {
        clearTimeout(timeout);
        lastErr = err;

        const code = err?.cause?.code;
        const transient =
          code === "ECONNRESET" ||
          code === "ETIMEDOUT" ||
          err?.name === "AbortError" ||
          err?.message?.includes("fetch failed");

        if (!transient || attempt === 4) {
          console.error(`[Bot] Batch fetch error:`, err.message);
          return signatures.map(() => null);
        }

        const delay = Math.min(3000, 300 * Math.pow(2, attempt - 1));
        console.error(`[Bot] Batch fetch transient error, retrying in ${delay}ms...`);
        await new Promise((r) => setTimeout(r, delay));
      }
    }

    console.error(`[Bot] Batch fetch failed after retries:`, lastErr?.message);
    return signatures.map(() => null);
  }

  private startPingLoop(): void {
    this.pingInterval = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.ping();
      }
    }, 30000); // Ping every 30 seconds
  }

  private stopPingLoop(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
  }

  private startLivenessCheck(): void {
    this.livenessCheckInterval = setInterval(() => {
      const now = Date.now();
      const pongAge = now - this.lastPongTime;
      const activityAge = now - this.lastActivityTime;

      // If no pong for 90s (3 missed pings) or no activity for 5min, kill connection
      if (pongAge > 90000 || activityAge > 300000) {
        console.log(
          `[Bot] Connection stale (pong: ${Math.round(pongAge / 1000)}s ago, activity: ${Math.round(activityAge / 1000)}s ago), force-closing...`
        );
        if (this.ws) {
          this.ws.terminate(); // Force kill
        }
      }
    }, 30000); // Check every 30 seconds
  }

  private stopLivenessCheck(): void {
    if (this.livenessCheckInterval) {
      clearInterval(this.livenessCheckInterval);
      this.livenessCheckInterval = null;
    }
  }

  private startPriceMonitoring(): void {
    const intervalMs = this.config.priceCheckIntervalMs ?? 5000;
    
    this.priceCheckInterval = setInterval(async () => {
      await this.checkPrices();
    }, intervalMs);
  }

  private stopPriceMonitoring(): void {
    if (this.priceCheckInterval) {
      clearInterval(this.priceCheckInterval);
      this.priceCheckInterval = null;
    }
  }

  private startTokenAccountMonitoring(): void {
    if (!this.config.tokenAccountSlLeader || !this.config.tokenAccountSlEnabled) return;
    
    const intervalMs = this.config.tokenAccountCheckIntervalMs ?? 15000;
    
    this.tokenAccountCheckInterval = setInterval(async () => {
      await this.checkTokenAccounts();
    }, intervalMs);
  }

  private stopTokenAccountMonitoring(): void {
    if (this.tokenAccountCheckInterval) {
      clearInterval(this.tokenAccountCheckInterval);
      this.tokenAccountCheckInterval = null;
    }
  }

  private async checkTokenAccounts(): Promise<void> {
    if (this.openPositions.size === 0) return;

    for (const [mint, position] of this.openPositions) {
      try {
        // Find leader's token account if not set
        if (!position.leaderTokenAccount && this.config.tokenAccountSlLeader) {
          const tokenAccount = await this.findLeaderTokenAccount(mint);
          if (tokenAccount) {
            position.leaderTokenAccount = tokenAccount;
            console.log(`[Bot] Found leader token account for ${mint.slice(0, 8)}...: ${tokenAccount.slice(0, 8)}...`);
          }
        }

        // Check Token Account SL (if leader token account found)
        if (position.leaderTokenAccount) {
          const tokenAccountSlHit = await this.checkTokenAccountSl(position);
          if (tokenAccountSlHit) {
            console.log(`[Bot] Token Account SL triggered for ${mint.slice(0, 8)}...`);
            await this.copySell(mint, "TOKEN_ACCOUNT_SL", 100);
          }
        }
      } catch (err: any) {
        // Silently skip on error
      }
    }
  }

  private async checkPrices(): Promise<void> {
    if (this.openPositions.size === 0) return;

    const tp1Trigger = this.config.tp1Trigger ?? 1.5;
    const tp1SellPercent = this.config.tp1SellPercent ?? 50;
    const tp2Trigger = this.config.tp2Trigger ?? 3.0;
    const slTrigger = this.config.slTrigger ?? 0.5;
    const smartSlCeiling = this.config.smartSlCeiling ?? 1.9;
    const smartSlFloor = this.config.smartSlFloor ?? 1.6;

    for (const [mint, position] of this.openPositions) {
      try {
        // Get current price via Jupiter quote using ORIGINAL amount (not remaining)
        // This ensures multiplier stays consistent after partial sells
        const quoteUrl = `https://api.jup.ag/ultra/v1/order?inputMint=${mint}&outputMint=${SOL_MINT}&amount=${position.originalTokenAmount.toString()}`;
        
        const resp = await fetch(quoteUrl, {
          headers: {
            "x-api-key": this.config.jupiterApiKey,
            Accept: "application/json",
          },
        });

        if (!resp.ok) continue;

        const quote = await resp.json() as any;
        const currentValueInSol = Number(quote?.outAmount ?? 0) / 1_000_000_000;
        
        if (!currentValueInSol || currentValueInSol === 0) {
          console.log(`[Bot] No quote for ${mint.slice(0, 8)}... (status: ${resp.status})`);
          continue;
        }

        const entryPrice = position.buyPriceInSol;
        const multiplier = currentValueInSol / entryPrice;

        // Log current price status for debugging
        console.log(`[Bot] Price check ${mint.slice(0, 8)}... ${multiplier.toFixed(2)}x (TP1: ${tp1Trigger}x, TP2: ${tp2Trigger}x, SL: ${slTrigger}x, SmartSL: ${smartSlCeiling}x→${smartSlFloor}x)`);

        // Update highest multiplier for smart SL ---
        if (multiplier > position.highestMultiplier) {
          position.highestMultiplier = multiplier;
        }

        // Check regular SL first (highest priority - sell remaining 100%)
        if (multiplier <= slTrigger) {
          console.log(`[Bot] SL hit for ${mint.slice(0, 8)}... (${multiplier.toFixed(2)}x <= ${slTrigger}x)`);
          await this.copySell(mint, "SL", 100); // Sell 100% of remaining
        }
        // Check Smart SL (price reached ceiling, then dropped to floor)
        else if (position.highestMultiplier >= smartSlCeiling && multiplier <= smartSlFloor) {
          console.log(`[Bot] Smart SL hit for ${mint.slice(0, 8)}... (high: ${position.highestMultiplier.toFixed(2)}x, now: ${multiplier.toFixed(2)}x <= ${smartSlFloor}x)`);
          await this.copySell(mint, "SMART_SL", 100); // Sell 100% of remaining
        }
        // Check TP2 (sell remaining 100%)
        else if (multiplier >= tp2Trigger) {
          console.log(`[Bot] TP2 hit for ${mint.slice(0, 8)}... (${multiplier.toFixed(2)}x >= ${tp2Trigger}x)`);
          await this.copySell(mint, "TP2", 100); // Sell 100% of remaining
        }
        // Check TP1 (partial sell - only if not already hit)
        else if (multiplier >= tp1Trigger && !position.tp1Hit) {
          console.log(`[Bot] TP1 hit for ${mint.slice(0, 8)}... (${multiplier.toFixed(2)}x >= ${tp1Trigger}x)`);
          await this.copySell(mint, "TP1", tp1SellPercent); // Sell X% of position
          position.tp1Hit = true; // Mark TP1 as hit
        }
      } catch (err: any) {
        // Silently skip on error - price might not be available yet
      }
    }
  }

  // Find token account for mint owned by TOKEN_ACCOUNT_LEADER
  private async findLeaderTokenAccount(mint: string): Promise<string | null> {
    if (!this.config.tokenAccountSlLeader) return null;

    const connection = new Connection(this.config.rpcUrl, "confirmed");
    const leaderPubkey = new PublicKey(this.config.tokenAccountSlLeader);
    const mintPubkey = new PublicKey(mint);

    // Try both standard and Token-2022 ATAs
    const standardAta = getAssociatedTokenAddressSync(
      mintPubkey,
      leaderPubkey,
      false,
      TOKEN_PROGRAM_ID
    );
    const ata2022 = getAssociatedTokenAddressSync(
      mintPubkey,
      leaderPubkey,
      false,
      TOKEN_2022_PROGRAM_ID
    );

    for (const ata of [standardAta, ata2022]) {
      try {
        const info = await connection.getAccountInfo(ata, "confirmed");
        if (info) {
          return ata.toBase58();
        }
      } catch {}
    }
    return null;
  }

  // Check token account transactions for SL trigger
  private async checkTokenAccountSl(position: FollowState): Promise<boolean> {
    if (!position.leaderTokenAccount || !this.config.tokenAccountSlLeader) return false;

    const url = new URL(`https://api-mainnet.helius-rpc.com/v0/addresses/${position.leaderTokenAccount}/transactions`);
    url.searchParams.set("api-key", this.heliusApiKey);
    url.searchParams.set("token-accounts", "none");
    url.searchParams.set("sort-order", "asc");
    
    if (position.lastCheckedSignature) {
      url.searchParams.set("after-signature", position.lastCheckedSignature);
    }

    try {
      const resp = await fetch(url.toString());
      if (!resp.ok) return false;

      const txs = await resp.json() as any[];
      if (!txs || txs.length === 0) return false;

      for (const tx of txs) {
        // Update last checked signature
        position.lastCheckedSignature = tx.signature;

        // Check for buy tx (nativeTransfers[0].amount ~ 2074080)
        if (tx.nativeTransfers && tx.nativeTransfers.length > 0) {
          const amount = tx.nativeTransfers[0].amount;
          // Check if it's a buy (amount around 2074080, with some tolerance)
          if (amount >= 2000000 && amount <= 2500000) {
            position.buyTxCount++;
            
            // Auto-collect known wallets from toUserAccount
            const toUserAccount = tx.nativeTransfers[0].toUserAccount;
            if (toUserAccount && !this.knownWalletSet.has(toUserAccount)) {
              this.knownWalletSet.add(toUserAccount);
              this.followerWalletSet.add(toUserAccount);
              console.log(`[Bot] Auto-added known wallet: ${toUserAccount.slice(0, 8)}... (from buy tx #${position.buyTxCount})`);
            }
            
            console.log(`[Bot] Token Account SL: Buy tx #${position.buyTxCount} detected for ${position.mint.slice(0, 8)}...`);
          }
        }

        // Check for sell tx (accountData[0].nativeBalanceChange ~ +/- 5000)
        if (tx.accountData && tx.accountData.length > 0) {
          const nativeChange = tx.accountData[0].nativeBalanceChange;
          // Check if it's a sell (only fee paid, ~5000)
          if (Math.abs(nativeChange) >= 4000 && Math.abs(nativeChange) <= 6000) {
            // Only trigger if we've seen at least 100 buy txs
            if (position.buyTxCount >= 100) {
              console.log(`[Bot] Token Account SL hit for ${position.mint.slice(0, 8)}... (sell tx detected after ${position.buyTxCount} buys)`);
              return true;
            }
          }
        }
      }
    } catch (err: any) {
      console.error(`[Bot] Token Account SL check error: ${err.message}`);
    }

    return false;
  }

  private startPoolMonitoring(): void {
    // Pool monitoring now uses per-position pool addresses from migration tx
    const intervalMs = this.config.poolCheckIntervalMs ?? 2000;
    
    this.poolCheckInterval = setInterval(async () => {
      await this.checkPool();
    }, intervalMs);
  }

  private stopPoolMonitoring(): void {
    if (this.poolCheckInterval) {
      clearInterval(this.poolCheckInterval);
      this.poolCheckInterval = null;
    }
  }

  // Calculate price from sqrtPrice (DAMM v2 uses sqrtPrice in Q64.64 format)
  private calculatePrice(sqrtPriceX64: bigint): number {
    const sqrtPrice = Number(sqrtPriceX64) / Math.pow(2, 64);
    return sqrtPrice * sqrtPrice;
  }

  // Get pool snapshot using CpAmm SDK
  private async getPoolSnapshot(poolAddress: string): Promise<PoolSnapshot | null> {
    try {
      const poolPubkey = new PublicKey(poolAddress);
      const state = await this.cpAmm.fetchPoolState(poolPubkey);
      
      return {
        tokenReserve: Number(state.tokenAAmount),
        quoteReserve: Number(state.tokenBAmount),
        price: this.calculatePrice(BigInt(state.sqrtPrice.toString())),
        liquidity: Number(state.liquidity),
        ts: Date.now(),
      };
    } catch (err: any) {
      console.error(`[Pool] Error fetching pool ${poolAddress.slice(0, 8)}...: ${err.message}`);
      return null;
    }
  }

  // Fetch transactions for a token mint (not pool) with wallet addresses
  // Filters out txs with amount 115624 (likely airdrops/fees)
  // Uses cursor-based paging with 'before' parameter
  private async fetchTokenTransactions(mint: string, beforeSignature?: string): Promise<PoolTx[]> {
    try {
      const url = new URL(`${HELIUS_BASE}/addresses/${mint}/transactions`);
      url.searchParams.set("api-key", this.heliusApiKey);
      url.searchParams.set("limit", "50");
      
      // Use 'before' cursor for pagination - fetches txs older than this signature
      if (beforeSignature) {
        url.searchParams.set("before", beforeSignature);
      }
      
      const resp = await fetch(url.toString());
      if (!resp.ok) return [];
      
      const txs = await resp.json() as any[];
      const result: PoolTx[] = [];
      
      for (const tx of txs) {
        // Determine side from tokenTransfers
        let side: "buy" | "sell" | "unknown" = "unknown";
        let amount = 0;
        let wallet: string | undefined;
        
        // Find the token transfer for this mint
        if (tx.tokenTransfers) {
          for (const transfer of tx.tokenTransfers) {
            if (transfer.mint === mint) {
              amount = transfer.tokenAmount || 0;
              
              // Filter out txs with amount 115624 (airdrops/fees)
              if (amount === 115624) {
                side = "unknown";
                continue;
              }
              
              wallet = transfer.fromUserAccount;
              
              // Check if this is a buy or sell by looking for SOL transfer
              const solTransfer = tx.tokenTransfers?.find((t: any) => t.mint === SOL_MINT);
              if (solTransfer) {
                // If SOL is going out from wallet, it's a BUY
                if (solTransfer.fromUserAccount === wallet) {
                  side = "buy";
                } else {
                  // If SOL is coming to wallet, it's a SELL
                  side = "sell";
                }
              } else {
                // No SOL transfer - check native transfers
                if (tx.nativeTransfers) {
                  for (const native of tx.nativeTransfers) {
                    if (native.amount >= 10000) { // Skip tiny amounts
                      if (native.fromUserAccount === wallet) {
                        side = "buy";
                        break;
                      } else if (native.toUserAccount === wallet) {
                        side = "sell";
                        break;
                      }
                    }
                  }
                }
              }
              break;
            }
          }
        }
        
        // Skip if we couldn't determine side or amount is filtered
        if (side === "unknown") continue;
        
        // Fallback: use feePayer as wallet if not found
        if (!wallet && tx.feePayer) {
          wallet = tx.feePayer;
        }
        
        result.push({
          signature: tx.signature,
          timestamp: tx.timestamp || Math.floor(Date.now() / 1000),
          wallet,
          side,
          amount,
        });
      }
      
      return result;
    } catch (err: any) {
      console.error(`[Token] Error fetching txs for ${mint.slice(0, 8)}...: ${err.message}`);
      return [];
    }
  }

  // Fetch transactions for a pool with wallet addresses
  // Uses cursor-based paging with 'before' parameter to avoid re-fetching
  private async fetchPoolTransactions(poolAddress: string, beforeSignature?: string): Promise<PoolTx[]> {
    try {
      const url = new URL(`${HELIUS_BASE}/addresses/${poolAddress}/transactions`);
      url.searchParams.set("api-key", this.heliusApiKey);
      url.searchParams.set("limit", "50");
      
      // Use 'before' cursor for pagination - fetches txs older than this signature
      if (beforeSignature) {
        url.searchParams.set("before", beforeSignature);
      }
      
      const resp = await fetch(url.toString());
      if (!resp.ok) return [];
      
      const txs = await resp.json() as any[];
      const result: PoolTx[] = [];
      
      for (const tx of txs) {
        // Determine side from multiple signals
        let side: "buy" | "sell" | "unknown" = "unknown";
        let amount = 0;
        let wallet: string | undefined;
        
        // Method 1: Check tokenTransfers for swap direction
        if (tx.tokenTransfers && tx.tokenTransfers.length >= 2) {
          const first = tx.tokenTransfers[0];
          const second = tx.tokenTransfers[1];
          
          if (first.mint === SOL_MINT) {
            // SOL out = BUY (user swapping SOL for token)
            side = "buy";
            amount = first.tokenAmount || 0;
            wallet = first.fromUserAccount;
          } else if (second.mint === SOL_MINT) {
            // SOL in = SELL (user swapping token for SOL)
            side = "sell";
            amount = second.tokenAmount || 0;
            wallet = first.fromUserAccount;
          }
        }
        
        // Method 2: Check nativeTransfers for SOL flow direction
        if (side === "unknown" && tx.nativeTransfers && tx.nativeTransfers.length > 0) {
          // Look for SOL transfers (amount > 0, native transfers are always SOL)
          for (const transfer of tx.nativeTransfers) {
            // Skip tiny amounts (likely fees)
            if (transfer.amount < 10000) continue;
            
            wallet = transfer.fromUserAccount;
            
            // If SOL is going TO the pool, it's a buy
            if (transfer.toUserAccount === poolAddress) {
              side = "buy";
              amount = transfer.amount;
              break;
            }
            // If SOL is coming FROM the pool, it's a sell
            if (transfer.fromUserAccount === poolAddress) {
              side = "sell";
              amount = transfer.amount;
              break;
            }
          }
        }
        
        // Method 3: Check accountData for token balance changes
        if (side === "unknown" && tx.accountData && tx.accountData.length >= 2) {
          // Look for token balance changes in the pool's token account
          for (const acc of tx.accountData) {
            if (acc.tokenBalanceChanges && acc.tokenBalanceChanges.length > 0) {
              for (const change of acc.tokenBalanceChanges) {
                // Positive token balance change = tokens added to account = SELL
                // Negative token balance change = tokens removed = BUY
                const rawAmount = change.rawTokenAmount?.tokenAmount;
                if (rawAmount) {
                  const tokenChange = BigInt(rawAmount);
                  if (tokenChange > 0n) {
                    // Tokens added to pool = someone sold
                    side = "sell";
                    amount = Math.abs(Number(tokenChange));
                    wallet = acc.account;
                  } else if (tokenChange < 0n) {
                    // Tokens removed from pool = someone bought
                    side = "buy";
                    amount = Math.abs(Number(tokenChange));
                    wallet = acc.account;
                  }
                }
              }
            }
          }
        }
        
        // Also check nativeTransfers for wallet if not found
        if (!wallet && tx.nativeTransfers && tx.nativeTransfers.length > 0) {
          wallet = tx.nativeTransfers[0].fromUserAccount;
        }
        
        // Fallback: use feePayer as wallet
        if (!wallet && tx.feePayer) {
          wallet = tx.feePayer;
        }
        
        result.push({
          signature: tx.signature,
          timestamp: tx.timestamp || Math.floor(Date.now() / 1000),
          wallet,
          side,
          amount,
        });
      }
      
      return result;
    } catch (err: any) {
      console.error(`[Pool] Error fetching txs for ${poolAddress.slice(0, 8)}...: ${err.message}`);
      return [];
    }
  }

  private async checkPool(): Promise<void> {
    const profitExitPercent = this.config.profitExitPercent ?? 1;
    const poolRpcUrl = this.config.poolRpcUrl ?? this.config.rpcUrl;
    const now = Date.now();

    // ===== PHASE 1: SIMPLE BUY FLOW - Fetch 250 txs, check insider % =====
    for (const [mint, pending] of this.pendingPositions) {
      try {
        // Get real pool state from CpAmm SDK
        const snapshot = await this.getPoolSnapshot(pending.poolAddress);
        if (!snapshot) continue;

        if (!pending.prevPoolSnapshot) {
          pending.prevPoolSnapshot = snapshot;
          console.log(`[Pool] ${mint.slice(0, 8)}... initialized price=${snapshot.price.toFixed(9)} tokenReserve=${snapshot.tokenReserve} quoteReserve=${snapshot.quoteReserve}`);
          continue;
        }

        // ===== FETCH FULL 250 TOKEN TRANSACTIONS IMMEDIATELY =====
        let tracker = this.earlyMetricsMap.get(mint);
        if (!tracker) {
          tracker = this.createEarlyMetrics(mint, pending.poolAddress);
          this.earlyMetricsMap.set(mint, tracker);
          console.log(`[EarlyScore] 🔄 Fetching first 250 token txs for ${mint.slice(0, 8)}...`);
        }

        // Fetch full 250 txs from TOKEN MINT address using pagination (if not done yet)
        if (!tracker.evaluated && tracker.txs.length < 250) {
          // Keep fetching batches until we have 250 txs
          let beforeSignature: string | undefined = undefined;
          let totalFetched = 0;
          let totalDups = 0;
          let totalFiltered = 0;
          
          while (tracker.txs.length < 250) {
            // Use TOKEN MINT address, not pool address
            const tokenTxs = await this.fetchTokenTransactions(mint, beforeSignature);
            if (tokenTxs.length === 0) break; // No more txs available
            
            let newTxs = 0;
            let dupTxs = 0;
            
            for (const tokenTx of tokenTxs) {
              // Deduplicate by signature
              if (tracker.seenSignatures.has(tokenTx.signature)) {
                dupTxs++;
                continue;
              }
              tracker.seenSignatures.add(tokenTx.signature);
              newTxs++;
              
              const earlyTx: EarlyTx = {
                signature: tokenTx.signature,
                ts: tokenTx.timestamp * 1000,
                from: tokenTx.wallet,
                side: tokenTx.side,
                amount: tokenTx.amount,
              };
              this.updateEarlyMetrics(tracker, earlyTx);
              
              // Log insider wallets (buy or sell)
              if (tokenTx.wallet && this.insiderWalletSet.has(tokenTx.wallet)) {
                console.log(`[EarlyScore] 🟪 INSIDER ${tokenTx.side.toUpperCase()}: ${tokenTx.wallet.slice(0, 8)}... (tx #${tracker.txs.length})`);
              }
              
              // Stop at 250
              if (tracker.txs.length >= 250) break;
            }
            
            totalFetched += tokenTxs.length;
            totalDups += dupTxs;
            
            // Set cursor for next batch (oldest tx signature)
            if (tokenTxs.length > 0) {
              beforeSignature = tokenTxs[tokenTxs.length - 1].signature;
            }
            
            // Log batch progress
            console.log(`[EarlyScore] 📦 Batch: fetched=${tokenTxs.length} new=${newTxs} dups=${dupTxs} | Total: ${tracker.txs.length}/250`);
            
            // Break if no new txs were added (reached end of history)
            if (newTxs === 0) break;
          }
          
          console.log(`[EarlyScore] 📊 Done fetching: ${tracker.txs.length} unique txs (fetched: ${totalFetched}, dups: ${totalDups}, filtered 115624 amounts)`);
        }

        // Check if we have 250 txs - make buy decision
        if (tracker.txs.length >= 250 && !tracker.evaluated) {
          // Count insider txs (both buy AND sell) from FULL token activity
          const insiderTxCount = tracker.txs.filter(tx => tx.from && this.insiderWalletSet.has(tx.from)).length;
          const insiderPercent = (insiderTxCount / tracker.txs.length) * 100;
          
          // Count insider buys vs sells
          const insiderBuys = tracker.txs.filter(tx => tx.from && this.insiderWalletSet.has(tx.from) && tx.side === "buy").length;
          const insiderSells = tracker.txs.filter(tx => tx.from && this.insiderWalletSet.has(tx.from) && tx.side === "sell").length;
          
          console.log(`[EarlyScore] ✅ ${mint.slice(0, 8)}... HAVE ${tracker.txs.length} TOKEN TXS`);
          console.log(`[EarlyScore] 📊 DOMINANCE: totalTokenTxs=${tracker.txs.length} | insiderTxs=${insiderTxCount} (${insiderBuys} buys, ${insiderSells} sells) | dominancePct=${insiderPercent.toFixed(1)}%`);
          
          if (insiderPercent >= 90) {
            // >90% insider txs (buy or sell) - BUY
            console.log(`[Bot] 🟢 INSIDER BUY TRIGGERED: ${mint.slice(0, 8)}... (${insiderPercent.toFixed(1)}% dominance >= 90% threshold)`);
            this.pendingPositions.delete(mint);
            this.attemptedBuys.add(mint);
            await this.copyBuyFromPending(pending, 1);
            continue;
          } else {
            // Not enough insider activity - reject
            console.log(`[Bot] 🔴 REJECT: ${mint.slice(0, 8)}... (only ${insiderPercent.toFixed(1)}% dominance, need 90%)`);
            tracker.evaluated = true;
            continue;
          }
        }
        
        // If we couldn't get 250 txs (token too new), reject
        if (tracker.txs.length < 250 && !tracker.evaluated) {
          console.log(`[Bot] ⏳ ${mint.slice(0, 8)}... Only ${tracker.txs.length} txs for token, waiting...`);
        }

        pending.prevPoolSnapshot = snapshot;
      } catch (err: any) {
        // Silently skip on error
      }
    }

    // ===== PHASE 2: Monitor OPEN positions (TP/SL exits only) =====
    for (const [mint, position] of this.openPositions) {
      // Skip if no pool address for this position
      if (!position.poolAddress) continue;

      try {
        // Get real pool state from CpAmm SDK
        const snapshot = await this.getPoolSnapshot(position.poolAddress);
        if (!snapshot) continue;

        if (!position.prevPoolSnapshot) {
          position.prevPoolSnapshot = snapshot;
          position.entryPrice = snapshot.price;
          console.log(`[Pool] ${mint.slice(0, 8)}... position initialized entryPrice=${snapshot.price.toFixed(9)}`);
          continue;
        }

        // Calculate profit
        if (position.entryPrice && position.entryPrice > 0) {
          const profitPct = ((snapshot.price - position.entryPrice) / position.entryPrice) * 100;
          
          if (profitPct > position.highestProfit) {
            position.highestProfit = profitPct;
          }

          // Log pool status
          console.log(`[Pool] ${mint.slice(0, 8)}... profit=${profitPct.toFixed(1)}% price=${snapshot.price.toFixed(9)} highestProfit=${position.highestProfit.toFixed(1)}%`);

          // ===== TP/SL EXIT LOGIC ONLY =====
          // TP1: Sell TP1_SELL_PERCENT at TP1_TRIGGER multiplier
          // TP2: Sell remaining at TP2_TRIGGER multiplier  
          // SL: Sell all at SL_TRIGGER multiplier
          // SmartSL: Trail stop loss from SMART_SL_CEILING to SMART_SL_FLOOR
          
          const tp1Trigger = this.config.tp1Trigger ?? 1.5;
          const tp1SellPercent = this.config.tp1SellPercent ?? 50;
          const tp2Trigger = this.config.tp2Trigger ?? 3.0;
          const slTrigger = this.config.slTrigger ?? 0.5;
          const smartSlCeiling = this.config.smartSlCeiling ?? 1.8;
          const smartSlFloor = this.config.smartSlFloor ?? 1.4;
          
          const currentMultiplier = snapshot.price / position.entryPrice;
          
          // Check TP1
          if (!position.tp1Hit && currentMultiplier >= tp1Trigger) {
            position.tp1Hit = true;
            console.log(`[Bot] 🎯 TP1 HIT: ${mint.slice(0, 8)}... (${(currentMultiplier * 100).toFixed(1)}% of entry, selling ${tp1SellPercent}%)`);
            await this.copySell(mint, "TP1", tp1SellPercent);
          }
          
          // Check TP2 (only after TP1)
          if (position.tp1Hit && currentMultiplier >= tp2Trigger) {
            console.log(`[Bot] 🎯 TP2 HIT: ${mint.slice(0, 8)}... (${(currentMultiplier * 100).toFixed(1)}% of entry, selling remaining)`);
            await this.copySell(mint, "TP2", 100);
            continue;
          }
          
          // Check Stop Loss
          if (currentMultiplier <= slTrigger) {
            console.log(`[Bot] 🛑 SL HIT: ${mint.slice(0, 8)}... (${(currentMultiplier * 100).toFixed(1)}% of entry)`);
            await this.copySell(mint, "STOP_LOSS", 100);
            continue;
          }
          
          // Smart Trailing Stop Loss
          if (position.highestProfit > smartSlCeiling * 100 - 100) {
            // We've reached ceiling, start trailing
            const trailingSl = position.highestProfit / 100 + 1 - (smartSlCeiling - smartSlFloor);
            const floorPrice = position.entryPrice * smartSlFloor;
            
            if (snapshot.price <= position.entryPrice * trailingSl && snapshot.price > floorPrice) {
              console.log(`[Bot] 🔒 SMART SL HIT: ${mint.slice(0, 8)}... (${(currentMultiplier * 100).toFixed(1)}% of entry, trailing from ${position.highestProfit.toFixed(1)}%)`);
              await this.copySell(mint, "SMART_SL", 100);
              continue;
            }
          }
        }

        position.prevPoolSnapshot = snapshot;
      } catch (err: any) {
        // Silently skip on error
      }
    }
  }

  // Buy from pending position (triggered by state machine)
  private async copyBuyFromPending(pending: PendingPosition, entryStage: 1 | 2 = 1): Promise<void> {
    console.log(`[Bot] Executing buy for ${pending.mint.slice(0, 8)}... (verdict: ENTRY_READY, stage=${entryStage})`);

    const connection = new Connection(this.config.rpcUrl, "confirmed");
    const owner = this.trader.ownerPubkey;

    // Derive both standard and Token-2022 ATAs
    const standardAta = getAssociatedTokenAddressSync(
      new PublicKey(pending.mint),
      owner,
      false,
      TOKEN_PROGRAM_ID
    );
    const ata2022 = getAssociatedTokenAddressSync(
      new PublicKey(pending.mint),
      owner,
      false,
      TOKEN_2022_PROGRAM_ID
    );

    // Helper to get balance from either ATA
    const getBalance = async (): Promise<{ ata: string; balance: bigint } | null> => {
      for (const ata of [standardAta, ata2022]) {
        try {
          const bal = await connection.getTokenAccountBalance(ata, "confirmed");
          if (bal?.value?.amount) {
            return { ata: ata.toBase58(), balance: BigInt(bal.value.amount) };
          }
        } catch {}
      }
      return null;
    };

    // Get balance BEFORE the buy
    const preResult = await getBalance();
    const preBalance = preResult?.balance ?? 0n;
    const botAta = preResult?.ata ?? standardAta.toBase58();

    const buyRes = await this.trader.buyToken({
      outputMint: pending.mint,
      amountLamports: this.config.buyAmountLamports,
    });

    if (!buyRes.success) {
      console.error(`[Bot] Buy failed: ${buyRes.error}`);
      return;
    }

    // Poll for balance AFTER the buy (up to 10 seconds)
    let postBalance = preBalance;
    let foundAta = botAta;
    
    for (let i = 0; i < 5; i++) {
      await new Promise((r) => setTimeout(r, 2000));
      
      const postResult = await getBalance();
      if (postResult && postResult.balance > preBalance) {
        postBalance = postResult.balance;
        foundAta = postResult.ata;
        break;
      }
      
      console.log(`[Bot] Waiting for token balance... (attempt ${i + 1}/5)`);
    }

    // Calculate delta (what the bot actually bought)
    const boughtDelta = postBalance > preBalance ? postBalance - preBalance : 0n;

    if (boughtDelta === 0n) {
      console.error(`[Bot] Warning: Could not determine tokens bought. Buy tx: ${buyRes.signature ?? "unknown"}`);
    }

    // Move from pending to open position
    this.openPositions.set(pending.mint, {
      mint: pending.mint,
      botAta: foundAta,
      leaderBuySignature: pending.leaderBuySignature,
      ourBuySignature: buyRes.signature,
      openedAtSlot: pending.slot,
      tokenAmountBought: boughtDelta,
      originalTokenAmount: boughtDelta,
      buyPriceInSol: Number(this.config.buyAmountLamports) / 1_000_000_000,
      tp1Hit: false,
      highestMultiplier: 1.0,
      leaderTokenAccount: undefined,
      lastCheckedSignature: undefined,
      buyTxCount: 0,
      // Pool monitoring state - continue from pending
      poolAddress: pending.poolAddress,
      prevPoolSnapshot: pending.prevPoolSnapshot,
      flipCount: 0,
      lastDirection: pending.lastDirection,
      entryPrice: pending.prevPoolSnapshot?.price,
      highestProfit: 0,
      // Enhanced momentum tracking
      momentumScore: 0,
      momentumHistory: [],
      momentumIncreasing: false,
      consecutiveBuys: 0,
      consecutiveSells: 0,
      dipsAbsorbed: 0,
      lastDipRecovery: 0,
      trendBroken: false,
      // Breakout protection
      intervalsSinceEntry: 0,
      peakMomentum: 0,
      intervalsSincePeakMomentum: 0,
      // Post-entry validation
      entryStage: entryStage,
      entryTime: Date.now(),
      postEntryValidated: false,
      earlyKillTriggered: false,
      // Insider movement tracking
      insiderBuysSinceEntry: 0,
      insiderSellsSinceEntry: 0,
      lastInsiderActivity: undefined,
      // Insider momentum tracking
      insiderBuysThisInterval: 0,
      insiderBuysPrevInterval: 0,
      peakInsiderBuyRate: 0,
    });

    console.log(
      `[Bot] ✅ Bought ${pending.mint}. Tracked bot amount: ${boughtDelta} tokens. Our tx: ${buyRes.signature ?? "unknown"}`
    );
  }

  private scheduleReconnect(): void {
    this.reconnectAttempts++;
    const delay = Math.min(30000, 1000 * Math.pow(2, this.reconnectAttempts - 1));
    console.log(`[Bot] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`);
    
    setTimeout(() => {
      if (!this.shouldStop) {
        this.connectWebSocket();
      }
    }, delay);
  }

  async stop(): Promise<void> {
    this.shouldStop = true;
    this.stopPingLoop();
    this.stopLivenessCheck();
    this.stopPriceMonitoring();
    this.stopTokenAccountMonitoring();
    this.stopPoolMonitoring();
    
    if (this.ws) {
      // Unsubscribe before closing
      if (this.subscriptionId !== null) {
        const unsubscribeMsg = {
          jsonrpc: "2.0",
          id: 2,
          method: "logsUnsubscribe",
          params: [this.subscriptionId],
        };
        this.ws.send(JSON.stringify(unsubscribeMsg));
      }
      this.ws.close();
      this.ws = null;
    }
    
    this.started = false;
    console.log(`[Bot] Stopped`);
  }

  private async fetchWithRetry(url: string, maxAttempts = 4): Promise<Response> {
    let lastErr: any;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);

      try {
        const resp = await fetch(url, {
          method: "GET",
          headers: { Accept: "application/json" },
          signal: controller.signal,
        });

        clearTimeout(timeout);
        return resp;
      } catch (err: any) {
        clearTimeout(timeout);
        lastErr = err;

        const code = err?.cause?.code;
        const transient =
          code === "ECONNRESET" ||
          code === "ETIMEDOUT" ||
          code === "UND_ERR_CONNECT_TIMEOUT" ||
          err?.name === "AbortError" ||
          err?.message?.includes("fetch failed");

        if (!transient || attempt === maxAttempts) {
          throw err;
        }

        const delay = Math.min(3000, 300 * 2 ** (attempt - 1));
        console.error(
          `[Bot] Transient fetch error (${code ?? err?.name ?? err?.message}), retrying in ${delay}ms...`
        );
        await new Promise((r) => setTimeout(r, delay));
      }
    }

    throw lastErr;
  }

  private async processTransaction(tx: HeliusTransaction): Promise<void> {
    const signature = tx.signature;

    // Handle METEORA_DAMM_V2 migration tx (UNKNOWN type, METEORA_DAMM_V2 source)
    // Extract pool address from nativeTransfers where amount = 8630400
    if (tx.type === "UNKNOWN" && tx.source === "METEORA_DAMM_V2") {
      const poolAddress = this.extractPoolAddress(tx);
      
      // Find the non-SOL mint from tokenTransfers
      const mint = tx.tokenTransfers?.find(t => t.mint !== SOL_MINT)?.mint;
      
      if (poolAddress && mint) {
        console.log(`[Bot] Detected METEORA_DAMM_V2 migration, pool: ${poolAddress.slice(0, 8)}... for mint: ${mint.slice(0, 8)}...`);
        
        // Store pool address for reference
        this.mintToPoolMap.set(mint, poolAddress);
        
        // Create pending position for state machine evaluation
        // State machine will decide if/when to buy
        const existingPending = this.pendingPositions.get(mint);
        const alreadyOpen = this.openPositions.has(mint);
        
        if (!existingPending && !alreadyOpen) {
          this.pendingPositions.set(mint, {
            mint,
            poolAddress,
            leaderBuySignature: signature,
            slot: tx.slot ?? 0,
            lastDirection: "NONE",
            lastPriceMove: 0,
          });
          console.log(`[Bot] Created pending position for ${mint.slice(0, 8)}... - early score engine evaluating...`);
        }
      }
      return;
    }

    // Handle TRANSFER type (normal swaps)
    if (tx.type === "TRANSFER" && tx.tokenTransfers?.length) {
      const signal = this.extractSwapSignal(tx);
      if (!signal) return;

      console.log(
        `[Bot] ${signal.side} ${signal.mint} (${signal.solAmount.toFixed(4)} SOL) in ${signal.signature.slice(0, 8)}...`
      );

      // Only track SELLs - BUYs are handled by state machine from migration tx
      // State machine will decide when to buy based on pool conditions
      if (signal.side === "SELL") {
        const alreadyOpen = this.openPositions.has(signal.mint);
        if (alreadyOpen) {
          console.log(`[Bot] Leader sold ${signal.mint.slice(0, 8)}... - our TP/SL will handle exit`);
        }
      } else {
        // BUY signal - check if pending position exists
        const pending = this.pendingPositions.get(signal.mint);
        if (!pending) {
          console.log(`[Bot] Leader bought ${signal.mint.slice(0, 8)}... - waiting for migration tx to start state machine`);
        }
      }
      return;
    }
  }

  // Extract pool address from METEORA_DAMM migration tx
  // Pool address is the toUserAccount in nativeTransfers where amount = 8630400
  private extractPoolAddress(tx: HeliusTransaction): string | null {
    const nativeTransfers = tx.nativeTransfers ?? [];
    
    for (const transfer of nativeTransfers) {
      // Pool creation has amount of 8630400 lamports
      if (transfer.amount === 8630400 && transfer.toUserAccount) {
        return transfer.toUserAccount;
      }
    }
    return null;
  }

  private extractSwapSignal(tx: HeliusTransaction): SwapSignal | null {
    const tokenTransfers = tx.tokenTransfers ?? [];
    if (tokenTransfers.length < 2) return null;

    // Index-based logic:
    // Index 0 = SOL mint -> BUY (leader pays SOL, receives token)
    // Index 1 = SOL mint -> SELL (leader receives SOL, pays token)
    const first = tokenTransfers[0];
    const second = tokenTransfers[1];

    let solTransfer: HeliusTokenTransfer;
    let tokenTransfer: HeliusTokenTransfer;
    let side: Side;

    if (first.mint === SOL_MINT) {
      // SOL at index 0 = BUY
      solTransfer = first;
      tokenTransfer = second;
      side = "BUY";
    } else if (second.mint === SOL_MINT) {
      // SOL at index 1 = SELL
      solTransfer = second;
      tokenTransfer = first;
      side = "SELL";
    } else {
      // No SOL transfer found
      return null;
    }

    return {
      signature: tx.signature,
      mint: tokenTransfer.mint,
      side,
      tokenDelta: BigInt(Math.floor(tokenTransfer.tokenAmount * 1e6)),
      solAmount: solTransfer.tokenAmount,
      slot: tx.slot,
    };
  }

  private async copyBuy(signal: SwapSignal): Promise<void> {
    console.log(`[Bot] Copy-buying ${signal.mint}...`);

    const connection = new Connection(this.config.rpcUrl, "confirmed");
    const owner = this.trader.ownerPubkey;

    // Derive both standard and Token-2022 ATAs
    const standardAta = getAssociatedTokenAddressSync(
      new PublicKey(signal.mint),
      owner,
      false,
      TOKEN_PROGRAM_ID
    );
    const ata2022 = getAssociatedTokenAddressSync(
      new PublicKey(signal.mint),
      owner,
      false,
      TOKEN_2022_PROGRAM_ID
    );

    // Helper to get balance from either ATA
    const getBalance = async (): Promise<{ ata: string; balance: bigint } | null> => {
      for (const ata of [standardAta, ata2022]) {
        try {
          const bal = await connection.getTokenAccountBalance(ata, "confirmed");
          if (bal?.value?.amount) {
            return { ata: ata.toBase58(), balance: BigInt(bal.value.amount) };
          }
        } catch {}
      }
      return null;
    };

    // Get balance BEFORE the buy
    const preResult = await getBalance();
    const preBalance = preResult?.balance ?? 0n;
    const botAta = preResult?.ata ?? standardAta.toBase58();

    const buyRes = await this.trader.buyToken({
      outputMint: signal.mint,
      amountLamports: this.config.buyAmountLamports,
    });

    if (!buyRes.success) {
      console.error(`[Bot] Copy buy failed: ${buyRes.error}`);
      return;
    }

    // Poll for balance AFTER the buy (up to 10 seconds)
    let postBalance = preBalance;
    let foundAta = botAta;
    
    for (let i = 0; i < 5; i++) {
      await new Promise((r) => setTimeout(r, 2000));
      
      const postResult = await getBalance();
      if (postResult && postResult.balance > preBalance) {
        postBalance = postResult.balance;
        foundAta = postResult.ata;
        break;
      }
      
      console.log(`[Bot] Waiting for token balance... (attempt ${i + 1}/5)`);
    }

    // Calculate delta (what the bot actually bought)
    const boughtDelta = postBalance > preBalance ? postBalance - preBalance : 0n;

    if (boughtDelta === 0n) {
      console.error(`[Bot] Warning: Could not determine tokens bought. Buy tx: ${buyRes.signature ?? "unknown"}`);
    }

    this.openPositions.set(signal.mint, {
      mint: signal.mint,
      botAta: foundAta,
      leaderBuySignature: signal.signature,
      ourBuySignature: buyRes.signature,
      openedAtSlot: signal.slot,
      tokenAmountBought: boughtDelta,
      originalTokenAmount: boughtDelta, // Keep original for price queries
      buyPriceInSol: Number(this.config.buyAmountLamports) / 1_000_000_000, // SOL spent
      tp1Hit: false, // Track if TP1 has been triggered
      highestMultiplier: 1.0, // Track highest multiplier for smart SL
      leaderTokenAccount: undefined, // Will be set when found
      lastCheckedSignature: undefined, // Will be set after first check
      buyTxCount: 0, // Count of buy txs in leader's token account
      // Pool monitoring state
      poolAddress: this.mintToPoolMap.get(signal.mint), // Get pool address from migration tx
      prevPoolSnapshot: undefined,
      flipCount: 0,
      lastDirection: "NONE",
      entryPrice: undefined,
      highestProfit: 0,
      // Enhanced momentum tracking
      momentumScore: 0,
      momentumHistory: [],
      momentumIncreasing: false,
      consecutiveBuys: 0,
      consecutiveSells: 0,
      dipsAbsorbed: 0,
      lastDipRecovery: 0,
      trendBroken: false,
      // Breakout protection
      intervalsSinceEntry: 0,
      peakMomentum: 0,
      intervalsSincePeakMomentum: 0,
      // Post-entry validation
      entryStage: 1, // Direct copy uses Stage 1
      entryTime: Date.now(),
      postEntryValidated: false,
      earlyKillTriggered: false,
      // Insider movement tracking
      insiderBuysSinceEntry: 0,
      insiderSellsSinceEntry: 0,
      lastInsiderActivity: undefined,
      // Insider momentum tracking
      insiderBuysThisInterval: 0,
      insiderBuysPrevInterval: 0,
      peakInsiderBuyRate: 0,
    });

    console.log(
      `[Bot] Bought ${signal.mint}. Tracked bot amount: ${boughtDelta} tokens. Our tx: ${buyRes.signature ?? "unknown"}`
    );
  }

  private async copySell(mint: string, trigger: string, sellPercent: number = 100): Promise<void> {
    const position = this.openPositions.get(mint);
    
    if (!position) {
      console.log(`[Bot] No position found for ${mint.slice(0, 8)}...`);
      return;
    }

    // Get actual token balance from wallet FIRST
    const connection = new Connection(this.config.rpcUrl, "confirmed");
    const owner = this.trader.ownerPubkey;
    
    // Try both standard and Token-2022 ATAs
    const standardAta = getAssociatedTokenAddressSync(
      new PublicKey(mint),
      owner,
      false,
      TOKEN_PROGRAM_ID
    );
    const ata2022 = getAssociatedTokenAddressSync(
      new PublicKey(mint),
      owner,
      false,
      TOKEN_2022_PROGRAM_ID
    );

    let actualBalance = 0n;
    for (const ata of [standardAta, ata2022]) {
      try {
        const bal = await connection.getTokenAccountBalance(ata, "confirmed");
        if (bal?.value?.amount) {
          actualBalance = BigInt(bal.value.amount);
          break;
        }
      } catch {}
    }

    // Skip if no balance
    if (actualBalance <= 0n) {
      console.log(`[Bot] No balance for ${mint.slice(0, 8)}... skipping sell (trigger: ${trigger})`);
      this.openPositions.delete(mint);
      this.soldTokens.add(mint);
      return;
    }

    const trackedAmount = position.tokenAmountBought;
    
    // Calculate amount to sell based on percentage (use min of actual and tracked)
    const maxSellAmount = actualBalance < trackedAmount ? actualBalance : trackedAmount;
    const amountToSell = (maxSellAmount * BigInt(Math.floor(sellPercent))) / 100n;
    
    if (amountToSell <= 0n) {
      console.log(`[Bot] No tokens to sell for ${mint.slice(0, 8)}... (tracked: ${trackedAmount}, actual: ${actualBalance}, sellPercent: ${sellPercent}%)`);
      this.openPositions.delete(mint);
      this.soldTokens.add(mint);
      return;
    }

    console.log(`[Bot] Selling ${amountToSell} tokens (${sellPercent}%) of ${mint.slice(0, 8)}... (tracked: ${trackedAmount}, actual: ${actualBalance}, trigger: ${trigger})`);

    const sellRes = await this.trader.sellTokenAmount({
      inputMint: mint,
      amount: amountToSell,
    });

    // Handle success
    if (sellRes.success) {
      // Update tracked amount (subtract sold amount)
      position.tokenAmountBought = trackedAmount - amountToSell;
      
      console.log(
        `[Bot] Sold ${amountToSell} tokens (${sellPercent}%) of ${mint.slice(0, 8)} (trigger: ${trigger}). Remaining: ${position.tokenAmountBought}. Our tx: ${sellRes.signature ?? "unknown"}`
      );

      // If sold 100% or no tokens remaining, close position
      if (sellPercent >= 100 || position.tokenAmountBought <= 0n) {
        this.openPositions.delete(mint);
        this.soldTokens.add(mint);
        this.attemptedBuys.delete(mint); // Allow re-buy after full sell
      }
    } else {
      console.error(`[Bot] Copy sell failed: ${sellRes.error}`);
      // Keep position tracking so we can retry sell later
    }
  }
}

function absBigInt(x: bigint): bigint {
  return x < 0n ? -x : x;
}

const LAMPORTS_PER_SOL = 1_000_000_000;

function solToLamports(sol: number | string): bigint {
  const solNum = typeof sol === "string" ? parseFloat(sol) : sol;
  return BigInt(Math.floor(solNum * LAMPORTS_PER_SOL));
}

// ------------------------
// bootstrap
// ------------------------
async function main() {
  const bot = new MeteoraDammV2CopyBot({
    rpcUrl: process.env.RPC_URL!,
    heliusApiKey: process.env.HELIUS_API_KEY!,
    privateKey: process.env.PRIVATE_KEY!,
    jupiterApiKey: process.env.JUPITER_API_KEY!,
    leaderWallet: process.env.LEADER_WALLET!,
    buyAmountLamports: solToLamports(process.env.BUY_AMOUNT_SOL ?? "0.01"),
    slippageBps: Number(process.env.SLIPPAGE_BPS ?? "1000"),
    priorityFeeLamports: Number(process.env.PRIORITY_FEE_LAMPORTS ?? "300000"),
    broadcastFeeType: (process.env.BROADCAST_FEE_TYPE as "maxCap" | "exactFee") ?? "exactFee",
    jitoTipLamports: Number(process.env.JITO_TIP_LAMPORTS ?? "0"),
    tp1Trigger: Number(process.env.TP1_TRIGGER ?? "1.5"),
    tp1SellPercent: Number(process.env.TP1_SELL_PERCENT ?? "50"),
    tp2Trigger: Number(process.env.TP2_TRIGGER ?? "3.0"),
    slTrigger: Number(process.env.SL_TRIGGER ?? "0.5"),
    smartSlCeiling: Number(process.env.SMART_SL_CEILING ?? "1.9"),
    smartSlFloor: Number(process.env.SMART_SL_FLOOR ?? "1.6"),
    tokenAccountSlLeader: process.env.TOKEN_ACCOUNT_LEADER,
    tokenAccountSlEnabled: process.env.TOKEN_ACCOUNT_SL_ENABLED === "true",
    tokenAccountCheckIntervalMs: Number(process.env.TOKEN_ACCOUNT_CHECK_INTERVAL_MS ?? "15000"),
    priceCheckIntervalMs: Number(process.env.PRICE_CHECK_INTERVAL_MS ?? "5000"),
    poolRpcUrl: process.env.POOL_RPC_URL,
    poolCheckIntervalMs: Number(process.env.POOL_CHECK_INTERVAL_MS ?? "2000"),
    profitExitPercent: Number(process.env.PROFIT_EXIT_PERCENT ?? "1"),
    // Early score engine wallets
    leaderWallets: process.env.LEADER_WALLETS?.split(",").map(w => w.trim()).filter(Boolean),
    followerWallets: process.env.FOLLOWER_WALLETS?.split(",").map(w => w.trim()).filter(Boolean),
    insiderWallets: process.env.INSIDER_WALLETS?.split(",").map(w => w.trim()).filter(Boolean),
  });

  await bot.start();

  process.on("SIGINT", async () => {
    console.log(`\n[Bot] Stopping...`);
    await bot.stop();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error(`[Bot] Fatal error:`, err);
  process.exit(1);
});
