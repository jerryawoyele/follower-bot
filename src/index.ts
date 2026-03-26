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

// Pool state machine
enum PoolState {
  AVOID = "AVOID",
  WATCH = "WATCH",
  ENTRY_READY = "ENTRY_READY",
  HOLD = "HOLD",
  EXIT = "EXIT",
}

// Pool snapshot for monitoring
type PoolSnapshot = {
  tokenReserve: number;
  quoteReserve: number;
  price: number;
  liquidity: number;
  ts: number;
};

// Pending position - migration detected but not yet bought
type PendingPosition = {
  mint: string;
  poolAddress: string;
  leaderBuySignature: string;
  slot: number;
  poolState: PoolState;
  prevPoolSnapshot?: PoolSnapshot;
  flipCount: number;
  lastDirection: "BUY" | "SELL" | "NONE";
  weakTrendCounter: number;
  recoveryFails: number;
  sellTrendIncreasing: boolean;
  lastSellMove: number;
  detectedAt: number; // timestamp
  // Momentum tracking
  consecutiveBuys: number;
  consecutiveSells: number;
  noMovementCount: number;
  lastImpact: number;
  impactHistory: number[]; // last 5 impacts
  lastPriceMove: number;
  priceMoveHistory: number[]; // last 5 price moves
  // Enhanced momentum tracking
  momentumScore: number; // Current momentum score
  momentumHistory: number[]; // last 5 momentum scores
  momentumIncreasing: boolean; // Is momentum increasing?
  dipsAbsorbed: number; // Count of dips that were absorbed (sell followed by stronger buy)
  lastDipRecovery: number; // Last dip recovery strength
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
  poolState: PoolState; // State machine state
  prevPoolSnapshot?: PoolSnapshot; // Previous pool snapshot for comparison
  flipCount: number; // Direction flip counter
  lastDirection: "BUY" | "SELL" | "NONE"; // Last detected direction
  weakTrendCounter: number; // Weak trend counter
  recoveryFails: number; // Recovery fail counter
  sellTrendIncreasing: boolean; // Is sell trend increasing?
  lastSellMove: number; // Last sell price move
  entryPrice?: number; // Price at entry (from pool)
  highestProfit: number; // Highest profit % reached
  // Enhanced momentum tracking for exits
  momentumScore: number;
  momentumHistory: number[];
  momentumIncreasing: boolean;
  consecutiveBuys: number;
  consecutiveSells: number;
  dipsAbsorbed: number;
  lastDipRecovery: number;
  trendBroken: boolean; // Has the trend definitively broken?
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
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    this.shouldStop = false;

    console.log(`[Bot] Leader wallet: ${this.leader}`);
    console.log(`[Bot] Bot wallet: ${this.trader.ownerPubkey.toBase58()}`);
    console.log(`[Bot] TP1: ${this.config.tp1Trigger ?? 1.5}x (${this.config.tp1SellPercent ?? 50}%) | TP2: ${this.config.tp2Trigger ?? 3.0}x (100%) | SL: ${this.config.slTrigger ?? 0.5}x`);
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

  private async checkPool(): Promise<void> {
    const profitExitPercent = this.config.profitExitPercent ?? 20;
    const poolRpcUrl = this.config.poolRpcUrl ?? this.config.rpcUrl;

    // ===== PHASE 1: Monitor PENDING positions (state machine decides entry) =====
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

        const prev = pending.prevPoolSnapshot;
        const dToken = snapshot.tokenReserve - prev.tokenReserve;
        const dQuote = snapshot.quoteReserve - prev.quoteReserve;

        const priceMovePct = (snapshot.price - prev.price) / prev.price;
        const quoteImpactPct = Math.abs(dQuote) / prev.quoteReserve;

        const buyPressure = dQuote > 0 && dToken < 0;
        const sellPressure = dQuote < 0 && dToken > 0;

        // Direction tracking
        const direction: "BUY" | "SELL" | "NONE" = buyPressure ? "BUY" : sellPressure ? "SELL" : "NONE";

        if (direction !== pending.lastDirection && pending.lastDirection !== "NONE") {
          pending.flipCount++;
        }
        pending.lastDirection = direction;

        // ===== MOMENTUM TRACKING =====
        // Consecutive buys/sells
        if (buyPressure) {
          pending.consecutiveBuys++;
          pending.consecutiveSells = 0;
        } else if (sellPressure) {
          pending.consecutiveSells++;
          pending.consecutiveBuys = 0;
        }

        // Dead token detection (no movement)
        if (quoteImpactPct < 0.001 && Math.abs(priceMovePct) < 0.001) {
          pending.noMovementCount++;
        } else {
          pending.noMovementCount = 0;
        }

        // Impact history for averaging
        pending.impactHistory.push(quoteImpactPct);
        if (pending.impactHistory.length > 5) pending.impactHistory.shift();
        const avgImpact = pending.impactHistory.reduce((a, b) => a + b, 0) / pending.impactHistory.length;

        // Price move history
        pending.priceMoveHistory.push(priceMovePct);
        if (pending.priceMoveHistory.length > 5) pending.priceMoveHistory.shift();

        // Impact acceleration (is impact increasing?)
        const impactAccelerating = pending.lastImpact > 0 && quoteImpactPct > pending.lastImpact;
        pending.lastImpact = quoteImpactPct;

        // Price recovery detection (dip followed by strong rebound)
        const hasRecovery = pending.priceMoveHistory.length >= 3 &&
          pending.priceMoveHistory.slice(-3).some(m => m < -0.01) && // had a dip
          priceMovePct > 0.02; // now recovering strongly

        // Weak trend detection
        if (buyPressure && pending.lastDirection === "SELL") {
          pending.weakTrendCounter++;
        }

        // Sell trend strength
        if (sellPressure) {
          if (Math.abs(priceMovePct) > Math.abs(pending.lastSellMove)) {
            pending.sellTrendIncreasing = true;
          }
          pending.lastSellMove = priceMovePct;
        }

        // No recovery check
        if (priceMovePct < 0) {
          pending.recoveryFails++;
        } else {
          pending.recoveryFails = 0;
        }

        // ===== MOMENTUM SCORE =====
        const momentumScore =
          pending.consecutiveBuys * 2 +
          avgImpact * 100 -
          pending.flipCount * 0.5;

        // ===== ENHANCED MOMENTUM TRACKING =====
        // Track momentum history
        pending.momentumHistory.push(momentumScore);
        if (pending.momentumHistory.length > 5) pending.momentumHistory.shift();
        
        // Detect momentum increasing (comparing to previous)
        const prevMomentum = pending.momentumHistory.length > 1 
          ? pending.momentumHistory[pending.momentumHistory.length - 2] 
          : 0;
        pending.momentumIncreasing = momentumScore > prevMomentum && momentumScore > 0;
        pending.momentumScore = momentumScore;

        // Dip absorption detection (sell followed by stronger buy)
        if (sellPressure && pending.consecutiveBuys === 0) {
          // Just saw a sell, mark potential dip
          pending.lastDipRecovery = -Math.abs(priceMovePct);
        } else if (buyPressure && pending.lastDipRecovery < 0) {
          // Buy after a dip - check if it absorbed the dip
          const recoveryStrength = priceMovePct - pending.lastDipRecovery;
          if (recoveryStrength > 0.01) { // Recovery stronger than dip
            pending.dipsAbsorbed++;
            pending.lastDipRecovery = priceMovePct;
          }
        }

        // ===== EXTREME EVENTS =====
        const extremeDump = priceMovePct < -0.5 || quoteImpactPct > 0.3;
        const possibleLiquidityPull =
          (prev.tokenReserve - snapshot.tokenReserve) / prev.tokenReserve > 0.2 &&
          (prev.quoteReserve - snapshot.quoteReserve) / prev.quoteReserve > 0.2;
        
        // ===== FIXED UNSTABLE DETECTION =====
        // Unstable only if lots of flips AND NO strong trend forming
        // Strong trend (consecBuys >= 3) OVERRIDES instability
        const hasStrongTrend = pending.consecutiveBuys >= 3;
        const hasMomentum = momentumScore > 0 && pending.momentumIncreasing;
        const isUnstable = pending.flipCount > 5 && avgImpact < 0.02 && !hasStrongTrend && !hasMomentum;
        
        // Dead token - no movement for 10+ intervals
        const isDeadToken = pending.noMovementCount >= 10;
        
        const weakTrend = pending.weakTrendCounter > 2 && !hasMomentum;
        const noRecovery = pending.recoveryFails > 3;

        // ===== TREND OVERRIDE CONDITIONS =====
        // Even if unstable, allow entry if strong momentum is building
        const trendOverride = 
          (pending.consecutiveBuys >= 5 && momentumScore > 0) ||
          (pending.consecutiveBuys >= 3 && avgImpact > 0.005 && pending.momentumIncreasing);

        // Strong trend = clustered buys with increasing impact
        const strongTrend = pending.consecutiveBuys >= 3 && avgImpact > 0.01;

        // ===== STATE MACHINE LOGIC (PRE-BUY) =====
        console.log(`[Pool] ${mint.slice(0, 8)}... state=${pending.poolState} price=${snapshot.price.toFixed(9)} move=${(priceMovePct * 100).toFixed(2)}% impact=${(quoteImpactPct * 100).toFixed(2)}% buy=${buyPressure} sell=${sellPressure} consecBuys=${pending.consecutiveBuys} momentum=${momentumScore.toFixed(1)} momInc=${pending.momentumIncreasing} dipsAbsorbed=${pending.dipsAbsorbed} unstable=${isUnstable} dead=${isDeadToken}`);
        
        switch (pending.poolState) {
          case PoolState.WATCH:
            // Entry conditions - need momentum building, not just single buy
            if (isDeadToken) {
              pending.poolState = PoolState.AVOID;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → AVOID (dead token: no movement for ${pending.noMovementCount} intervals)`);
            } else if (extremeDump || possibleLiquidityPull) {
              pending.poolState = PoolState.AVOID;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → AVOID (dump=${(priceMovePct * 100).toFixed(1)}%)`);
            } else if (trendOverride) {
              // TREND OVERRIDE: Strong momentum overrides instability
              pending.poolState = PoolState.ENTRY_READY;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → ENTRY_READY (TREND OVERRIDE: consecBuys=${pending.consecutiveBuys} momentum=${momentumScore.toFixed(1)} increasing=${pending.momentumIncreasing})`);
            } else if (isUnstable && !trendOverride) {
              pending.poolState = PoolState.AVOID;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → AVOID (unstable without trend)`);
            } else if (
              (strongTrend || (buyPressure && quoteImpactPct > 0.03 && impactAccelerating)) &&
              !weakTrend
            ) {
              pending.poolState = PoolState.ENTRY_READY;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → ENTRY_READY (momentum: consecBuys=${pending.consecutiveBuys} impact=${(avgImpact * 100).toFixed(1)}% accelerating=${impactAccelerating})`);
            }
            break;

          case PoolState.ENTRY_READY:
            // TRIGGER BUY!
            console.log(`[Bot] 🟢 ENTRY VERDICT: Buying ${mint.slice(0, 8)}... (momentumScore=${momentumScore.toFixed(1)})`);
            this.pendingPositions.delete(mint);
            this.attemptedBuys.add(mint);
            await this.copyBuyFromPending(pending);
            break;

          case PoolState.AVOID:
            // Reset to WATCH if conditions improve
            if (!isUnstable && !isDeadToken && buyPressure && !weakTrend) {
              pending.poolState = PoolState.WATCH;
              // Reset counters
              pending.flipCount = 0;
              pending.weakTrendCounter = 0;
              pending.recoveryFails = 0;
              pending.sellTrendIncreasing = false;
              pending.consecutiveBuys = 0;
              pending.consecutiveSells = 0;
              pending.noMovementCount = 0;
              pending.impactHistory = [];
              pending.priceMoveHistory = [];
              pending.momentumHistory = [];
              pending.momentumScore = 0;
              pending.momentumIncreasing = false;
              pending.dipsAbsorbed = 0;
              pending.lastDipRecovery = 0;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → WATCH (reset)`);
            }
            break;
        }

        pending.prevPoolSnapshot = snapshot;
        pending.lastPriceMove = priceMovePct;
      } catch (err: any) {
        // Silently skip on error
      }
    }

    // ===== PHASE 2: Monitor OPEN positions (state machine decides exit) =====
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

        const prev = position.prevPoolSnapshot;
        const dToken = snapshot.tokenReserve - prev.tokenReserve;
        const dQuote = snapshot.quoteReserve - prev.quoteReserve;

        const priceMovePct = (snapshot.price - prev.price) / prev.price;
        const quoteImpactPct = Math.abs(dQuote) / prev.quoteReserve;

        const buyPressure = dQuote > 0 && dToken < 0;
        const sellPressure = dQuote < 0 && dToken > 0;

        // Direction tracking
        const direction: "BUY" | "SELL" | "NONE" = buyPressure ? "BUY" : sellPressure ? "SELL" : "NONE";

        if (direction !== position.lastDirection && position.lastDirection !== "NONE") {
          position.flipCount++;
        }
        position.lastDirection = direction;

        // ===== ENHANCED MOMENTUM TRACKING FOR EXITS =====
        // Consecutive buys/sells
        if (buyPressure) {
          position.consecutiveBuys++;
          position.consecutiveSells = 0;
        } else if (sellPressure) {
          position.consecutiveSells++;
          position.consecutiveBuys = 0;
        }

        // Calculate momentum score
        const momentumScore = position.consecutiveBuys * 2 - position.consecutiveSells * 1.5 - position.flipCount * 0.3;
        
        // Track momentum history
        position.momentumHistory.push(momentumScore);
        if (position.momentumHistory.length > 5) position.momentumHistory.shift();
        
        // Detect momentum direction
        const prevMomentum = position.momentumHistory.length > 1 
          ? position.momentumHistory[position.momentumHistory.length - 2] 
          : 0;
        position.momentumIncreasing = momentumScore > prevMomentum;
        position.momentumScore = momentumScore;

        // Dip absorption for exits
        if (sellPressure && position.consecutiveBuys === 0) {
          position.lastDipRecovery = -Math.abs(priceMovePct);
        } else if (buyPressure && position.lastDipRecovery < 0) {
          const recoveryStrength = priceMovePct - position.lastDipRecovery;
          if (recoveryStrength > 0.01) {
            position.dipsAbsorbed++;
            position.lastDipRecovery = priceMovePct;
          }
        }

        // Weak trend detection
        if (buyPressure && position.lastDirection === "SELL") {
          position.weakTrendCounter++;
        }

        // Sell trend strength
        if (sellPressure) {
          if (Math.abs(priceMovePct) > Math.abs(position.lastSellMove)) {
            position.sellTrendIncreasing = true;
          }
          position.lastSellMove = priceMovePct;
        }

        // No recovery check
        if (priceMovePct < 0) {
          position.recoveryFails++;
        } else {
          position.recoveryFails = 0;
        }

        // Calculate profit
        if (position.entryPrice && position.entryPrice > 0) {
          const profitPct = ((snapshot.price - position.entryPrice) / position.entryPrice) * 100;
          
          if (profitPct > position.highestProfit) {
            position.highestProfit = profitPct;
          }

          // ===== IMPROVED PROFIT EXIT LOGIC =====
          // Only exit when profit > target AND trend is breaking
          const trendBreaking = 
            position.consecutiveBuys === 0 && 
            position.consecutiveSells >= 2 &&
            !position.momentumIncreasing;
          
          if (profitPct >= profitExitPercent && trendBreaking) {
            console.log(`[Bot] PROFIT EXIT: ${mint.slice(0, 8)}... (${profitPct.toFixed(1)}% profit, trend breaking)`);
            await this.copySell(mint, "PROFIT_EXIT", 100);
            continue;
          }
        }

        // Extreme events
        const extremeDump = priceMovePct < -0.5 || quoteImpactPct > 0.3;
        const possibleLiquidityPull =
          (prev.tokenReserve - snapshot.tokenReserve) / prev.tokenReserve > 0.2 &&
          (prev.quoteReserve - snapshot.quoteReserve) / prev.quoteReserve > 0.2;
        
        // ===== TREND BREAK DETECTION =====
        // Trend is broken when:
        // 1. consecBuys reset to 0 AND
        // 2. momentum dropping AND  
        // 3. no recovery after 3+ intervals
        position.trendBroken = 
          position.consecutiveBuys === 0 &&
          !position.momentumIncreasing &&
          position.recoveryFails > 3 &&
          position.dipsAbsorbed === 0; // No dips absorbed = no buyers

        const isUnstable = position.flipCount > 5 && !position.momentumIncreasing;
        const weakTrend = position.weakTrendCounter > 2 && !position.momentumIncreasing;
        const noRecovery = position.recoveryFails > 3;

        // ===== STATE MACHINE LOGIC (POST-BUY) =====
        console.log(`[Pool] ${mint.slice(0, 8)}... state=${position.poolState} profit=${position.highestProfit.toFixed(1)}% price=${snapshot.price.toFixed(9)} move=${(priceMovePct * 100).toFixed(2)}% impact=${(quoteImpactPct * 100).toFixed(2)}% buy=${buyPressure} sell=${sellPressure} consecBuys=${position.consecutiveBuys} consecSells=${position.consecutiveSells} momentum=${position.momentumScore.toFixed(1)} trendBroken=${position.trendBroken}`);
        
        switch (position.poolState) {
          case PoolState.WATCH:
            if (buyPressure && quoteImpactPct > 0.03 && !isUnstable) {
              position.poolState = PoolState.ENTRY_READY;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → ENTRY_READY`);
            }
            break;

          case PoolState.ENTRY_READY:
            position.poolState = PoolState.HOLD;
            console.log(`[Bot] Pool ${mint.slice(0, 8)}... → HOLD`);
            break;

          case PoolState.HOLD:
            // HARD EXIT: Only for extreme events
            if (extremeDump || possibleLiquidityPull) {
              console.log(`[Bot] HARD EXIT: Dump detected for ${mint.slice(0, 8)}...`);
              await this.copySell(mint, "HARD_EXIT", 100);
              position.poolState = PoolState.EXIT;
            } 
            // SOFT EXIT: Only when trend is definitively broken
            else if (position.trendBroken) {
              console.log(`[Bot] SOFT EXIT: Trend broken for ${mint.slice(0, 8)}... (consecBuys=${position.consecutiveBuys} consecSells=${position.consecutiveSells} momentum=${position.momentumScore.toFixed(1)})`);
              await this.copySell(mint, "SOFT_EXIT", 100);
              position.poolState = PoolState.EXIT;
            }
            // Allow dips to recover - don't exit on single sell
            break;

          case PoolState.EXIT:
            position.poolState = PoolState.AVOID;
            console.log(`[Bot] Pool ${mint.slice(0, 8)}... → AVOID`);
            break;

          case PoolState.AVOID:
            if (!isUnstable && buyPressure) {
              position.poolState = PoolState.WATCH;
              // Reset counters
              position.flipCount = 0;
              position.weakTrendCounter = 0;
              position.recoveryFails = 0;
              position.sellTrendIncreasing = false;
              console.log(`[Bot] Pool ${mint.slice(0, 8)}... → WATCH (reset)`);
            }
            break;
        }

        position.prevPoolSnapshot = snapshot;
      } catch (err: any) {
        // Silently skip on error
      }
    }
  }

  // Buy from pending position (triggered by state machine)
  private async copyBuyFromPending(pending: PendingPosition): Promise<void> {
    console.log(`[Bot] Executing buy for ${pending.mint.slice(0, 8)}... (verdict: ENTRY_READY)`);

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
      poolState: PoolState.HOLD, // Already in position, start in HOLD
      prevPoolSnapshot: pending.prevPoolSnapshot,
      flipCount: pending.flipCount,
      lastDirection: pending.lastDirection,
      weakTrendCounter: pending.weakTrendCounter,
      recoveryFails: pending.recoveryFails,
      sellTrendIncreasing: pending.sellTrendIncreasing,
      lastSellMove: pending.lastSellMove,
      entryPrice: pending.prevPoolSnapshot?.price,
      highestProfit: 0,
      // Enhanced momentum tracking - continue from pending
      momentumScore: pending.momentumScore,
      momentumHistory: pending.momentumHistory,
      momentumIncreasing: pending.momentumIncreasing,
      consecutiveBuys: pending.consecutiveBuys,
      consecutiveSells: pending.consecutiveSells,
      dipsAbsorbed: pending.dipsAbsorbed,
      lastDipRecovery: pending.lastDipRecovery,
      trendBroken: false,
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
            poolState: PoolState.WATCH,
            flipCount: 0,
            lastDirection: "NONE",
            weakTrendCounter: 0,
            recoveryFails: 0,
            sellTrendIncreasing: false,
            lastSellMove: 0,
            detectedAt: Date.now(),
            // Momentum tracking
            consecutiveBuys: 0,
            consecutiveSells: 0,
            noMovementCount: 0,
            lastImpact: 0,
            impactHistory: [],
            lastPriceMove: 0,
            priceMoveHistory: [],
            // Enhanced momentum tracking
            momentumScore: 0,
            momentumHistory: [],
            momentumIncreasing: false,
            dipsAbsorbed: 0,
            lastDipRecovery: 0,
          });
          console.log(`[Bot] Created pending position for ${mint.slice(0, 8)}... - state machine evaluating...`);
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
      poolState: PoolState.WATCH, // Start in WATCH state
      prevPoolSnapshot: undefined,
      flipCount: 0,
      lastDirection: "NONE",
      weakTrendCounter: 0,
      recoveryFails: 0,
      sellTrendIncreasing: false,
      lastSellMove: 0,
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
    profitExitPercent: Number(process.env.PROFIT_EXIT_PERCENT ?? "20"),
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
