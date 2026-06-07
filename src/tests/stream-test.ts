import Client, { SubscribeRequest } from "@triton-one/yellowstone-grpc";

enum SlotStatus {
  SLOT_PROCESSED = 0,
  SLOT_CONFIRMED = 1,
  SLOT_FINALIZED = 2,
  SLOT_FIRST_SHRED_RECEIVED = 3,
  SLOT_COMPLETED = 4,
  SLOT_CREATED_BANK = 5,
  SLOT_DEAD = 6,
  UNRECOGNIZED = -1
}
import { Connection, Keypair } from "@solana/web3.js";
import * as fs from "fs";
import * as path from "path";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

const RPC_URL = process.env.SOLANA_RPC_URL || "https://api.devnet.solana.com";
const GRPC_URL = process.env.GRPC_URL;
const GRPC_TOKEN = process.env.GRPC_TOKEN;
const WALLET_PATH = process.env.WALLET_KEYPAIR_PATH || "./devnet-wallet.json";

function getStatusString(status: SlotStatus): string {
  switch (status) {
    case SlotStatus.SLOT_PROCESSED: return "PROCESSED";
    case SlotStatus.SLOT_CONFIRMED: return "CONFIRMED";
    case SlotStatus.SLOT_FINALIZED: return "FINALIZED";
    case SlotStatus.SLOT_FIRST_SHRED_RECEIVED: return "FIRST_SHRED_RECEIVED";
    case SlotStatus.SLOT_COMPLETED: return "COMPLETED";
    case SlotStatus.SLOT_CREATED_BANK: return "CREATED_BANK";
    case SlotStatus.SLOT_DEAD: return "DEAD";
    default: return "UNKNOWN";
  }
}

async function main() {
  console.log("==================================================");
  console.log("       YELLOWSTONE GRPC STANDALONE TEST SCRIPT     ");
  console.log("==================================================");

  if (!GRPC_URL) {
    console.error("FAIL: GRPC_URL environment variable is not set.");
    process.exit(1);
  }

  // Resolve wallet keypair path to get the public key
  let walletPublicKey = "";
  const absoluteWalletPath = path.resolve(WALLET_PATH);
  if (fs.existsSync(absoluteWalletPath)) {
    try {
      const secretKey = Uint8Array.from(JSON.parse(fs.readFileSync(absoluteWalletPath, "utf-8")));
      const keypair = Keypair.fromSecretKey(secretKey);
      walletPublicKey = keypair.publicKey.toBase58();
      console.log(`Loaded wallet public key: ${walletPublicKey}`);
    } catch (e: any) {
      console.warn(`Could not parse wallet keypair: ${e.message}. Using fallback.`);
    }
  }
  if (!walletPublicKey) {
    // Fallback dummy pubkey if no keypair file exists
    walletPublicKey = "Dngb3Tss5cZqQvj9rG94sL7z344J23t4R2h7R68QyM8K";
    console.log(`Using fallback wallet public key: ${walletPublicKey}`);
  }

  const connection = new Connection(RPC_URL, "confirmed");
  const client = new Client(GRPC_URL, GRPC_TOKEN || undefined, undefined);

  // Health and Metric collection variables
  const slotUpdates: { slot: number; status: SlotStatus; timestamp: number; leader: string }[] = [];
  const slotTimestamps: number[] = [];
  let matchingTransactionsSeen = 0;
  let gapsDetected = 0;
  let expectedSlotsCount = 0;
  let backpressureTriggered = false;
  let disconnectSimulated = false;
  let reconnectSuccessful = false;
  let reconnectTimeMs = 0;

  // Callback to fetch leader info asynchronously
  const getLeaderName = async (slot: number): Promise<string> => {
    try {
      return await connection.getSlotLeader(slot);
    } catch {
      return "Unknown (RPC Fail)";
    }
  };

  // Reconnection and Stream Management
  let stream: any = null;
  let isConnected = false;
  let disconnectTime = 0;

  const connectStream = async () => {
    try {
      console.log(`Connecting to Yellowstone gRPC at ${GRPC_URL}...`);
      await client.connect();
      stream = await client.subscribe();
      isConnected = true;

      // Handle reconnect confirmation
      if (disconnectSimulated) {
        reconnectTimeMs = Date.now() - disconnectTime;
        reconnectSuccessful = true;
        console.log(`\n[RECONNECT] Stream reconnected successfully in ${reconnectTimeMs}ms!`);
      }

      stream.on("data", async (data: any) => {
        // Monitor backpressure
        if (stream.readableLength > stream.readableHighWaterMark) {
          backpressureTriggered = true;
        }

        if (data.slot) {
          const slotNum = parseInt(data.slot.slot);
          const timestamp = Date.now();
          slotTimestamps.push(timestamp);

          // For the first 20 slots, log and record full detail
          if (slotUpdates.length < 20) {
            const leader = await getLeaderName(slotNum);
            const statusStr = getStatusString(data.slot.status);
            const update = {
              slot: slotNum,
              status: data.slot.status,
              timestamp,
              leader
            };
            slotUpdates.push(update);
            console.log(`[SLOT] #${slotUpdates.length.toString().padStart(2, "0")} | Slot: ${slotNum.toString().padEnd(9)} | Status: ${statusStr.padEnd(10)} | Leader: ${leader.padEnd(44)}`);
          }
        }

        if (data.transaction) {
          matchingTransactionsSeen++;
          const sig = Buffer.from(data.transaction.transaction.signature).toString("hex");
          console.log(`[TRANSACTION] Seen wallet transaction signature: ${sig}`);
        }
      });

      stream.on("error", (err: any) => {
        console.warn(`[STREAM ERROR] ${err.message || err}`);
        handleConnectionLoss();
      });

      stream.on("end", () => {
        console.log("[STREAM END] Connection ended by server.");
        handleConnectionLoss();
      });

      // Send subscribe request
      const subscribeRequest: SubscribeRequest = {
        slots: {
          "slots-sub": {
            filterByCommitment: true
          }
        },
        accounts: {},
        transactions: {
          "wallet-txs": {
            accountInclude: [walletPublicKey],
            accountExclude: [],
            accountRequired: [],
            vote: false,
            failed: false
          }
        },
        transactionsStatus: {},
        blocks: {},
        blocksMeta: {},
        entry: {},
        accountsDataSlice: []
      };

      await new Promise<void>((resolve, reject) => {
        stream.write(subscribeRequest, (err: any) => {
          if (err) reject(err);
          else resolve();
        });
      });
      console.log("gRPC stream subscribed to slots & wallet transactions successfully.");
    } catch (e: any) {
      console.error(`Connection failed: ${e.message}`);
      handleConnectionLoss();
    }
  };

  const handleConnectionLoss = () => {
    if (!isConnected) return;
    isConnected = false;
    console.log("Connection lost. Retrying in 1 second...");
    setTimeout(connectStream, 1000);
  };

  // Start the connection
  await connectStream();

  // Wait 30 seconds to collect initial slots, then simulate a disconnect
  console.log("\nCollecting data. Will simulate a disconnect in 30 seconds...");
  await new Promise((resolve) => setTimeout(resolve, 30000));

  // Verify initial slot updates received
  const initialSlotsPass = slotUpdates.length >= 10;
  console.log(`\n--- Verification: Initial Slots ---\n[${initialSlotsPass ? "PASS" : "FAIL"}] Received ${slotUpdates.length} slots (expected at least 10).`);

  // Simulate disconnect
  console.log("\n--- Simulating Network Disconnect ---");
  disconnectSimulated = true;
  disconnectTime = Date.now();
  if (stream) {
    stream.destroy();
  }
  isConnected = false;

  // Wait 10 seconds to allow the ReconnectionManager to reconnect and run
  console.log("Waiting 10 seconds for reconnection check...");
  await new Promise((resolve) => setTimeout(resolve, 10000));

  // Reconnection check verification
  const reconnectPass = reconnectSuccessful && reconnectTimeMs <= 5000;
  console.log(`\n--- Verification: Reconnection ---`);
  console.log(`[${reconnectPass ? "PASS" : "FAIL"}] Reconnection Manager triggered.`);
  console.log(`- Reconnect Success: ${reconnectSuccessful}`);
  console.log(`- Reconnect Time: ${reconnectTimeMs}ms (Limit: 5000ms)`);

  // Clean up connection
  if (stream) {
    stream.destroy();
  }

  // Calculate Health Metrics
  console.log("\n==================================================");
  console.log("                 HEALTH SUMMARY                   ");
  console.log("==================================================");

  let avgSlotTime = 0;
  let uptimePercentage = 0;

  if (slotTimestamps.length > 1) {
    const intervals: number[] = [];
    for (let i = 1; i < slotTimestamps.length; i++) {
      const diff = slotTimestamps[i]! - slotTimestamps[i - 1]!;
      // Filter out simulated disconnect interval (which will spike the duration)
      if (diff < 3000) {
        intervals.push(diff);
      }
    }

    if (intervals.length > 0) {
      const sum = intervals.reduce((a, b) => a + b, 0);
      avgSlotTime = sum / intervals.length;
    }

    // Check for gaps
    for (let i = 1; i < slotUpdates.length; i++) {
      if (slotUpdates[i]!.slot > slotUpdates[i - 1]!.slot + 1) {
        gapsDetected += (slotUpdates[i]!.slot - slotUpdates[i - 1]!.slot - 1);
      }
    }

    // Uptime expectation: ~400ms per slot
    const totalTimeCollected = slotTimestamps[slotTimestamps.length - 1]! - slotTimestamps[0]!;
    expectedSlotsCount = Math.floor(totalTimeCollected / 400);
    uptimePercentage = Math.min(100, Math.floor((slotTimestamps.length / expectedSlotsCount) * 100));
  }

  console.log(`- Average Slot Interval: ${avgSlotTime.toFixed(1)} ms`);
  console.log(`- Gaps Detected in Slot Sequence: ${gapsDetected} slots`);
  console.log(`- Stream Uptime: ${uptimePercentage}% (slots received: ${slotTimestamps.length}, expected: ${expectedSlotsCount})`);
  console.log(`- Backpressure Triggered: ${backpressureTriggered}`);
  console.log(`- Wallet Transactions Monitored: ${matchingTransactionsSeen}`);

  const healthPass = avgSlotTime > 0 && avgSlotTime < 1000;
  console.log(`\n[${healthPass ? "PASS" : "FAIL"}] Stream Health Metrics within acceptable boundaries.`);

  console.log("==================================================");
  
  if (initialSlotsPass && reconnectPass && healthPass) {
    console.log("ALL CHECKS PASSED: Yellowstone gRPC stream is fully operational.");
    process.exit(0);
  } else {
    console.error("FAIL: One or more checks failed. Please check endpoint configurations and network status.");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Fatal test error:", err);
  process.exit(1);
});
