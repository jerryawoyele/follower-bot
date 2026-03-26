# Fly.io Deployment Guide

## Prerequisites

1. Install flyctl CLI:
   ```powershell
   iwr https://fly.io/install.ps1 -useb | iex
   ```

2. Login to Fly.io:
   ```bash
   fly auth login
   ```

## Deployment Steps

### 1. Launch the app
```bash
fly launch
```
- Choose a region close to Solana RPC nodes (e.g., `sjc` for US West)
- When asked about Dockerfile, select "Yes" (it will detect your Dockerfile)

### 2. Set secrets (environment variables)
Import your .env file as secrets:
```powershell
Get-Content .env | fly secrets import
```

Or set individually:
```bash
fly secrets set RPC_URL="https://beta.helius-rpc.com/?api-key=YOUR_KEY"
fly secrets set HELIUS_API_KEY="your_key"
fly secrets set PRIVATE_KEY="your_private_key"
fly secrets set JUPITER_API_KEY="your_jupiter_key"
fly secrets set LEADER_WALLET="Fx87hHhHtfp47KPwQTzsNdfGrUmKv3ihtxHA3rxbPPrd"
fly secrets set BUY_AMOUNT_SOL="0.001"
fly secrets set POLLING_INTERVAL_MS="1000"
fly secrets set SLIPPAGE_BPS="10000"
fly secrets set PRIORITY_FEE_LAMPORTS="50000"
fly secrets set BROADCAST_FEE_TYPE="maxCap"
```

### 3. Deploy
```bash
fly deploy
```

## Useful Commands

- **View logs**: `fly logs`
- **Check status**: `fly status`
- **SSH into machine**: `fly ssh console`
- **Scale up**: `fly scale count 1`
- **Destroy app**: `fly apps destroy <app-name>`

## Notes

- Your bot is a **background process** (no HTTP server), which is fine for Fly.io
- The bot will restart automatically if it crashes
- Use `fly logs` to monitor the bot's output
- Make sure `.env` is in `.gitignore` (secrets are stored securely on Fly.io)
