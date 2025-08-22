import { spawn, ChildProcess } from "child_process";

const pythonProcesses: ChildProcess[] = [];
let cleanupListenersAdded = false;

export const spawnPythonProcess = async (exchange: string, tokens?: string[]) => {
  const args = ["./src/ccxtPyClaude5.py", exchange];
  if (tokens && tokens.length > 0) {
    args.push(tokens.join(","));
  }
  
  const pythonProcess = spawn(
    "./venv/bin/python3",
    args,
    {
      stdio: "inherit", // Inherit stdio to see Python output directly
    }
  );

  console.log(
    `Spawned Python process for ${exchange} with PID: ${pythonProcess.pid}`
  );

  // Add to our tracking array
  pythonProcesses.push(pythonProcess);

  pythonProcess.on("exit", (code, signal) => {
    console.log(
      `Python process for ${exchange} exited with code ${code}, signal ${signal}`
    );
    // Remove from tracking array
    const index = pythonProcesses.indexOf(pythonProcess);
    if (index > -1) {
      pythonProcesses.splice(index, 1);
    }
  });

  // Only add cleanup listeners once
  if (!cleanupListenersAdded) {
    const cleanup = async () => {
      console.log(`Gracefully shutting down ${pythonProcesses.length} Python processes...`);
      
      // Send SIGTERM to all processes
      const shutdownPromises = pythonProcesses.map(async (process) => {
        if (process && !process.killed) {
          console.log(`Sending SIGTERM to PID: ${process.pid}`);
          process.kill("SIGTERM");
          
          // Wait up to 10 seconds for graceful shutdown
          return new Promise<void>((resolve) => {
            const timeout = setTimeout(() => {
              if (!process.killed) {
                console.log(`Force killing PID: ${process.pid} after timeout`);
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
