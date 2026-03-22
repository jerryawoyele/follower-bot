# Meteora DAMM v2 Copy Bot

A bot that follows Meteora DAMM v2 swaps from a target wallet using Jupiter Ultra API for real-time copy trading.

## How It Works

1. **Monitors** the target wallet via WebSocket logs for new transactions
2. **Detects** DAMM v2 swaps by checking if the transaction involves the Meteora DAMM v2 program
3. **Infers** the swap direction (BUY/SELL) from token balance deltas
4. **Copies** buys immediately using Jupiter Ultra API
5. **Sells** 100% of your position when the leader sells that same token

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

**Required Environment Variables:**

| Variable | Description |
|----------|-------------|
| `RPC_URL` | Solana RPC URL (use a high-quality RPC like Helius, QuickNode, or Triton) |
| `PRIVATE_KEY` | Your bot wallet's private key (base58 encoded) |
| `JUPITER_API_KEY` | Your Jupiter API key |
| `LEADER_WALLET` | The wallet address to follow/copy |
| `BUY_AMOUNT_LAMPORTS` | Amount of SOL per copied buy (in lamports) |

### 3. Run the Bot

```bash
# Development
npm run dev

# Production
npm run build
npm start
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `SLIPPAGE_BPS` | 1000 (10%) | Slippage tolerance in basis points |
| `PRIORITY_FEE_LAMPORTS` | 300000 | Priority fee for transactions |
| `BROADCAST_FEE_TYPE` | exactFee | Fee type: "maxCap" or "exactFee" |
| `JITO_TIP_LAMPORTS` | 0 | Jito tip for faster execution |
| `sellOnAnyNextSwapForMint` | false | If true, sells on any next swap; if false, only sells on leader SELL |

## Important Notes

### Performance
- Use a high-quality RPC endpoint for faster transaction detection
- Consider using Jito tips for faster transaction execution
- The bot only reacts to transactions **after** it starts (no historical processing)

### Safety
- Start with small `BUY_AMOUNT_LAMPORTS` to test
- Monitor the bot's behavior before increasing amounts
- The bot tracks positions by mint - restarts will lose track of open positions

### Limitations
- Side detection uses token balance deltas, which may be confused by:
  - Zaps or wrapper transactions
  - Multi-mint transactions
  - ATA creation fees
- Consider adding additional filters for:
  - Minimum liquidity checks
  - Maximum slippage enforcement
  - Token blacklists

## Program IDs

- **Meteora DAMM v2**: `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG`
- **Jupiter Ultra API**: `https://api.jup.ag/ultra/v1`

## License

MIT
