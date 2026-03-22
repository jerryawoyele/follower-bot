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

  // polling interval in ms (default: 500ms = 2 req/sec)
  pollingIntervalMs?: number;
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

type FollowState = {
  mint: string;
  botAta: string;
  leaderBuySignature: string;
  ourBuySignature?: string;
  openedAtSlot: number;
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

    const orderResp = await fetch(`${ULTRA_BASE}/order?${qs.toString()}`, {
      method: "GET",
      headers: {
        "x-api-key": this.jupiterApiKey,
        Accept: "application/json",
      },
    });

    const orderJson = (await orderResp.json()) as JupiterOrderResponse;
    if (!orderResp.ok) {
      return {
        success: false,
        error: `Order HTTP ${orderResp.status}: ${JSON.stringify(orderJson)}`,
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
}

export class MeteoraDammV2CopyBot {
  private readonly config: Required<
    Pick<BotConfig, "pollingIntervalMs">
  > &
    BotConfig;

  private readonly trader: JupiterUltraTrader;
  private readonly leader: string;
  private readonly heliusApiKey: string;

  private pollingTimer: ReturnType<typeof setInterval> | null = null;
  private lastSignature: string | null = null;
  private readonly processedSignatures = new Set<string>();
  private readonly openPositions = new Map<string, FollowState>();
  private readonly attemptedBuys = new Set<string>(); // Track mints we've already tried to buy
  private readonly soldTokens = new Set<string>(); // Track mints we've sold to prevent re-buying
  private started = false;
  private consecutive429s = 0;
  private isPolling = false;

  constructor(config: BotConfig) {
    this.config = {
      ...config,
      pollingIntervalMs: config.pollingIntervalMs ?? 500, // 500ms = 2 req/sec, safe for free tier
    };
    this.trader = new JupiterUltraTrader(config);
    this.leader = config.leaderWallet;
    this.heliusApiKey = config.heliusApiKey;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    console.log(`[Bot] Leader wallet: ${this.leader}`);
    console.log(`[Bot] Bot wallet: ${this.trader.ownerPubkey.toBase58()}`);
    console.log(`[Bot] Polling interval: ${this.config.pollingIntervalMs}ms`);

    // Get the latest signature to start polling from
    await this.initializeLastSignature();

    console.log(`[Bot] Starting HTTP polling from signature: ${this.lastSignature?.slice(0, 8)}...`);

    // Start polling
    this.pollingTimer = setInterval(async () => {
      await this.poll();
    }, this.config.pollingIntervalMs);
  }

  private async initializeLastSignature(): Promise<void> {
    const url = new URL(`${HELIUS_BASE}/addresses/${this.leader}/transactions`);
    url.searchParams.set("api-key", this.heliusApiKey);

    const resp = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });

    if (!resp.ok) {
      console.error(`[Bot] Failed to initialize: Helius HTTP ${resp.status}`);
      return;
    }

    const txs = (await resp.json()) as HeliusTransaction[];
    if (txs && txs.length > 0) {
      // Get the most recent signature (first in default desc order)
      this.lastSignature = txs[0].signature;
    }
  }

  async stop(): Promise<void> {
    if (this.pollingTimer !== null) {
      clearInterval(this.pollingTimer);
      this.pollingTimer = null;
    }
    this.started = false;
    console.log(`[Bot] Stopped`);
  }

  private async poll(): Promise<void> {
    // Skip if already polling (prevent overlap)
    if (this.isPolling) return;
    this.isPolling = true;

    try {
      await this.fetchAndProcessTransactions();
    } catch (err) {
      console.error(`[Bot] Poll error:`, err);
    } finally {
      this.isPolling = false;
    }
  }

  private async fetchAndProcessTransactions(): Promise<void> {
    const url = new URL(`${HELIUS_BASE}/addresses/${this.leader}/transactions`);
    url.searchParams.set("api-key", this.heliusApiKey);
    url.searchParams.set("sort-order", "asc");

    if (this.lastSignature) {
      url.searchParams.set("after-signature", this.lastSignature);
    }

    const resp = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });

    if (!resp.ok) {
      if (resp.status === 429) {
        this.consecutive429s++;
        const backoffMs = Math.min(5000, 1000 * Math.pow(2, this.consecutive429s - 1));
        console.error(`[Bot] Rate limited (429), backing off for ${backoffMs}ms...`);
        await new Promise((r) => setTimeout(r, backoffMs));
      } else {
        console.error(`[Bot] Helius HTTP ${resp.status}`);
      }
      return;
    }

    this.consecutive429s = 0;

    const txs = (await resp.json()) as HeliusTransaction[];
    if (!txs || txs.length === 0) return;

    // With ascending order, transactions are already in chronological order
    // Update lastSignature to the last one in the response for next pagination
    this.lastSignature = txs[txs.length - 1].signature;

    for (const tx of txs) {
      const signature = tx.signature;
      if (!signature || this.processedSignatures.has(signature)) continue;
      this.processedSignatures.add(signature);

      // Skip failed transactions
      if (tx.transactionError) continue;

      // Handle TRANSFER type (normal swaps)
      if (tx.type === "TRANSFER" && tx.tokenTransfers?.length) {
        const signal = this.extractSwapSignal(tx);
        if (!signal) continue;

        console.log(
          `[Bot] ${signal.side} ${signal.mint} (${signal.solAmount.toFixed(4)} SOL) in ${signal.signature.slice(0, 8)}...`
        );

        const tracked = this.openPositions.get(signal.mint);
        const alreadyBought = this.attemptedBuys.has(signal.mint) || this.soldTokens.has(signal.mint);

        // Only buy if we don't have a position AND haven't already bought/sold this mint
        if (!tracked && !alreadyBought && signal.side === "BUY") {
          this.attemptedBuys.add(signal.mint);
          await this.copyBuy(signal);
        }
        // Note: We don't sell on subsequent swaps - only sell at migration
        continue;
      }

      // Handle UNKNOWN type with METEORA_DAMM_V2 source (migration)
      // This is the only sell trigger
      if (tx.type === "UNKNOWN" && tx.source === "METEORA_DAMM_V2" && tx.tokenTransfers?.length) {
        // Get token mint from tokenTransfers[1]
        const tokenTransfer = tx.tokenTransfers[1];
        if (tokenTransfer?.mint && tokenTransfer.mint !== SOL_MINT) {
          const mint = tokenTransfer.mint;
          const tracked = this.openPositions.get(mint);

          if (tracked) {
            console.log(`[Bot] Migration detected for ${mint.slice(0, 8)}..., selling position...`);
            await this.copySell(mint, signature);
          }
        }
        continue;
      }
    }
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

    const buyRes = await this.trader.buyToken({
      outputMint: signal.mint,
      amountLamports: this.config.buyAmountLamports,
    });

    if (!buyRes.success) {
      console.error(`[Bot] Copy buy failed: ${buyRes.error}`);
      return;
    }

    const botAta = this.trader.getAtaForMint(signal.mint);

    this.openPositions.set(signal.mint, {
      mint: signal.mint,
      botAta,
      leaderBuySignature: signal.signature,
      ourBuySignature: buyRes.signature,
      openedAtSlot: signal.slot,
    });

    console.log(
      `[Bot] Bought ${signal.mint}. Our tx: ${buyRes.signature ?? "unknown"}`
    );
  }

  private async copySell(mint: string, leaderExitSignature: string): Promise<void> {
    console.log(`[Bot] Selling 100% of ${mint}...`);

    const sellRes = await this.trader.sellToken100Percent({ inputMint: mint });

    // Always clean up position tracking, even if sell failed
    this.openPositions.delete(mint);
    this.soldTokens.add(mint); // Prevent re-buying this mint

    if (!sellRes.success) {
      console.error(`[Bot] Copy sell failed: ${sellRes.error}`);
      return;
    }

    console.log(
      `[Bot] Sold ${mint}. Our tx: ${sellRes.signature ?? "unknown"} | Leader exit tx: ${leaderExitSignature}`
    );
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
    pollingIntervalMs: Number(process.env.POLLING_INTERVAL_MS ?? "500"),
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
