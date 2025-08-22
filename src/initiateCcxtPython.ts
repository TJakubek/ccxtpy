import { spawn, ChildProcess } from "child_process";

interface ProcessInfo {
  process: ChildProcess;
  exchange: string;
  tokens: string[];
  restartCount: number;
  lastRestart: number;
}

const pythonProcesses: ProcessInfo[] = [];
let cleanupListenersAdded = false;
let shutdownInitiated = false;

const createPythonProcess = (
  exchange: string,
  tokens: string[] = []
): ProcessInfo => {
  const args = ["./src/ccxtPyClaude5.py", exchange];
  if (tokens.length > 0) {
    args.push(tokens.join(","));
  }

  const pythonProcess = spawn("./venv/bin/python3", args, {
    stdio: "inherit",
  });

  console.log(
    `✅ Spawned Python process for ${exchange} with PID: ${pythonProcess.pid}`
  );

  return {
    process: pythonProcess,
    exchange,
    tokens,
    restartCount: 0,
    lastRestart: Date.now(),
  };
};

const shouldRestart = (
  processInfo: ProcessInfo,
  code: number | null,
  signal: NodeJS.Signals | null
): boolean | null => {
  // Don't restart if shutdown was initiated
  if (shutdownInitiated) return false;

  // Don't restart if process was killed intentionally (SIGTERM, SIGINT)
  if (signal === "SIGTERM" || signal === "SIGINT") return false;

  // Don't restart if too many restarts in short time (exponential backoff)
  const now = Date.now();
  const timeSinceLastRestart = now - processInfo.lastRestart;
  const minRestartInterval = Math.min(
    5000 * Math.pow(2, processInfo.restartCount),
    300000
  ); // Cap at 5 minutes

  if (timeSinceLastRestart < minRestartInterval) {
    console.log(
      `⏳ Too many restarts for ${processInfo.exchange}, waiting ${Math.round(minRestartInterval / 1000)}s before next attempt`
    );
    return false;
  }

  // Restart if process crashed (non-zero exit code) or unexpected signal
  return code !== 0 || (signal && !["SIGTERM", "SIGINT"].includes(signal));
};

const restartProcess = async (processInfo: ProcessInfo) => {
  console.log(
    `🔄 Restarting Python process for ${processInfo.exchange} (restart #${processInfo.restartCount + 1})`
  );

  const newProcessInfo = createPythonProcess(
    processInfo.exchange,
    processInfo.tokens
  );
  newProcessInfo.restartCount = processInfo.restartCount + 1;
  newProcessInfo.lastRestart = Date.now();

  setupProcessHandlers(newProcessInfo);

  // Replace old process info with new one
  const index = pythonProcesses.findIndex(
    (p) => p.exchange === processInfo.exchange
  );
  if (index > -1) {
    pythonProcesses[index] = newProcessInfo;
  } else {
    pythonProcesses.push(newProcessInfo);
  }
};

const setupProcessHandlers = (processInfo: ProcessInfo) => {
  processInfo.process.on("exit", async (code, signal) => {
    const { exchange } = processInfo;
    console.log(
      `💀 Python process for ${exchange} exited with code ${code}, signal ${signal}`
    );

    // Remove from tracking array
    const index = pythonProcesses.findIndex((p) => p.exchange === exchange);
    if (index > -1) {
      pythonProcesses.splice(index, 1);
    }

    // Check if we should restart
    if (shouldRestart(processInfo, code, signal)) {
      console.log(`🚨 Unexpected exit for ${exchange}, scheduling restart...`);
      setTimeout(() => restartProcess(processInfo), 1000); // Small delay before restart
    } else {
      console.log(
        `✅ Process ${exchange} terminated normally, no restart needed`
      );
    }
  });

  processInfo.process.on("error", (error) => {
    console.error(`❌ Process error for ${processInfo.exchange}:`, error);
  });
};

export const spawnPythonProcess = async (
  exchange: string,
  tokens?: string[]
) => {
  const processInfo = createPythonProcess(exchange, tokens || []);
  setupProcessHandlers(processInfo);
  pythonProcesses.push(processInfo);

  // Only add cleanup listeners once
  if (!cleanupListenersAdded) {
    const cleanup = async () => {
      shutdownInitiated = true; // Prevent any restart attempts
      console.log(
        `Gracefully shutting down ${pythonProcesses.length} Python processes...`
      );

      // Send SIGTERM to all processes
      const shutdownPromises = pythonProcesses.map(async (processInfo) => {
        const process = processInfo.process;
        if (process && !process.killed) {
          console.log(
            `Sending SIGTERM to ${processInfo.exchange} (PID: ${process.pid})`
          );
          process.kill("SIGTERM");

          // Wait up to 10 seconds for graceful shutdown
          return new Promise<void>((resolve) => {
            const timeout = setTimeout(() => {
              if (!process.killed) {
                console.log(
                  `Force killing ${processInfo.exchange} (PID: ${process.pid}) after timeout`
                );
                process.kill("SIGKILL");
              }
              resolve();
            }, 10000);

            process.on("exit", () => {
              clearTimeout(timeout);
              resolve();
            });
          });
        }
      });

      try {
        await Promise.all(shutdownPromises);
        console.log("✅ All Python processes shut down gracefully");
      } catch (error) {
        console.error("❌ Error during shutdown:", error);
      }

      process.exit(0);
    };

    // Capture termination signals
    process.on("SIGINT", cleanup); // Ctrl+C
    process.on("SIGTERM", cleanup); // Kill command

    cleanupListenersAdded = true;
  }
};
