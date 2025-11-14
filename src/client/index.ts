import axios from "axios";
import chalk from "chalk";
import readline from "readline";

const PORT = parseInt(process.env.PORT || "3000");
const API_URL = `http://localhost:${PORT}`;

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function sendNotification(channel: string, message: string) {
  try {
    const response = await axios.post(`${API_URL}/notify`, {
      userId: "test_user",
      channel,
      message,
      title: `Test ${channel} notification`,
    });

    console.log(chalk.green(`✓ Sent ${channel}: ${response.data.result.id}`));
  } catch (error: any) {
    console.log(
      chalk.red(
        `✗ Failed ${channel}: ${error.response?.data?.error || error.message}`
      )
    );
  }
}

async function bulkSend(count: number) {
  console.log(`\nSending ${count} notifications...`);
  const channels = ["sms", "email", "push"];

  const promises = [];
  for (let i = 0; i < count; i++) {
    const channel = channels[i % 3];
    promises.push(sendNotification(channel, `Bulk message ${i + 1}`));
  }

  await Promise.all(promises);

  // Show stats
  const stats = await axios.get(`${API_URL}/stats`);
  console.log("\n📊 Stats:", stats.data);
}

function showMenu() {
  console.log("\n" + chalk.blue("=== Notification Service Test Client ==="));
  console.log("1. Send single notification");
  console.log("2. Send bulk notifications");
  console.log("3. Show stats");
  console.log("4. Exit");
}

async function main() {
  while (true) {
    showMenu();
    const choice = await new Promise<string>((resolve) => {
      rl.question("\nChoice: ", resolve);
    });

    switch (choice) {
      case "1":
        const channel = await new Promise<string>((resolve) => {
          rl.question("Channel (sms/email/push): ", resolve);
        });
        await sendNotification(channel, "Test message");
      case "2":
        const count = await new Promise<string>((resolve) => {
          rl.question("How many notifications: ", resolve);
        });
        await bulkSend(parseInt(count));
        break;
      case "3":
        const stats = await axios.get(`${API_URL}/stats`);
        console.log("\n📊 Current Stats:", stats.data);
        break;
      case "4":
        process.exit(0);
      default:
        console.log("Invalid choice");
    }
  }
}

main();
