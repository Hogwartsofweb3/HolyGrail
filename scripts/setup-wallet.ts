import { Keypair, Connection, LAMPORTS_PER_SOL } from "@solana/web3.js";
import * as fs from "fs";
import * as path from "path";
import dotenv from "dotenv";

// Load environment variables if available
dotenv.config();

const WALLET_PATH = process.env.WALLET_KEYPAIR_PATH || "./devnet-wallet.json";
const RPC_URL = process.env.SOLANA_RPC_URL || "https://api.devnet.solana.com";

async function main() {
  console.log("=== Solana Devnet Wallet Setup ===");
  
  const absolutePath = path.resolve(WALLET_PATH);
  
  let keypair: Keypair;
  
  if (fs.existsSync(absolutePath)) {
    console.log(`Loading existing keypair from ${absolutePath}...`);
    try {
      const secretKeyString = fs.readFileSync(absolutePath, "utf-8");
      const secretKey = Uint8Array.from(JSON.parse(secretKeyString));
      keypair = Keypair.fromSecretKey(secretKey);
    } catch (error) {
      console.error("Failed to parse existing keypair file. Generating a new one instead.");
      keypair = Keypair.generate();
    }
  } else {
    console.log("Generating a new keypair...");
    keypair = Keypair.generate();
    const directory = path.dirname(absolutePath);
    if (!fs.existsSync(directory)) {
      fs.mkdirSync(directory, { recursive: true });
    }
    fs.writeFileSync(
      absolutePath,
      JSON.stringify(Array.from(keypair.secretKey)),
      "utf-8"
    );
    console.log(`Saved new keypair to ${absolutePath}`);
  }
  
  console.log(`Public Key: ${keypair.publicKey.toBase58()}`);
  
  console.log(`Connecting to RPC: ${RPC_URL}...`);
  const connection = new Connection(RPC_URL, "confirmed");
  
  try {
    const balance = await connection.getBalance(keypair.publicKey);
    console.log(`Current Balance: ${balance / LAMPORTS_PER_SOL} SOL`);
    
    console.log("Requesting 2 SOL airdrop...");
    const signature = await connection.requestAirdrop(
      keypair.publicKey,
      2 * LAMPORTS_PER_SOL
    );
    
    console.log("Waiting for airdrop confirmation...");
    const latestBlockHash = await connection.getLatestBlockhash();
    await connection.confirmTransaction({
      blockhash: latestBlockHash.blockhash,
      lastValidBlockHeight: latestBlockHash.lastValidBlockHeight,
      signature: signature,
    }, "confirmed");
    
    const newBalance = await connection.getBalance(keypair.publicKey);
    console.log(`New Balance: ${newBalance / LAMPORTS_PER_SOL} SOL`);
    console.log("Airdrop complete!");
  } catch (error: any) {
    console.error("Error occurred during airdrop:");
    console.error(error.message || error);
    console.log("\nIf the airdrop failed, it is likely due to rate-limiting on devnet.");
    console.log(`You can manually request airdrops to this address: ${keypair.publicKey.toBase58()}`);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
});
