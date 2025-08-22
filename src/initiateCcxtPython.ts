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
    const cleanup = () => {
      console.log(`Killing ${pythonProcesses.length} Python processes...`);
      pythonProcesses.forEach((process) => {
        if (process && !process.killed) {
          process.kill("SIGTERM");
        }
      });
      process.exit();
    };

    // Capture termination signals
    process.on("SIGINT", cleanup); // Ctrl+C
    process.on("SIGTERM", cleanup); // Kill command
    process.on("exit", cleanup); // Exit triggered from anywhere

    cleanupListenersAdded = true;
  }
};
