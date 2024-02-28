import express, { Request, Response } from "express";
import { MongoClient, Db, Collection } from "mongodb";
import bodyParser from "body-parser";
import { config } from "./configs";

const app = express();
const port = 3002;
const cors = require("cors");

app.use(bodyParser.json());
app.use(cors({ origin: "*" }));

// MongoDB Connection Setup
const mongoURI: string = config.MONGO_URI;
const dbName: string = config.DB_NAME;
const emailQueueCollectionName: string = "emailQueue";

interface EmailPayload {
  // Define the structure of your email payload here
  // Example:
  to: string;
  subject: string;
  body: string;
}

interface EmailQueueEntry {
  _id: string;
  payload: EmailPayload;
  sent: boolean;
  retried: boolean;
  retryCount: number;
}

class EmailQueue {
  private collection: Collection<any>;

  constructor(db: Db) {
    this.collection = db.collection(emailQueueCollectionName);
  }

  async enqueue(emailPayload: EmailPayload): Promise<void> {
    await this.collection.insertOne({ payload: emailPayload, sent: false, retried: false, retryCount: 0 });
  }

  async dequeue(): Promise<EmailQueueEntry | null> {
    const result: any = await this.collection.findOneAndUpdate(
      { sent: false, retried: false, retryCount: { $lt: 3 } },
      { $set: { retried: true }, $inc: { retryCount: 1 } },
      { sort: { _id: 1 } }
    );
    return result.value || null;
  }

  async markAsSent(entry: EmailQueueEntry): Promise<void> {
    await this.collection.updateOne({ _id: entry._id }, { $set: { sent: true } });
  }
  async markAsNotSent(entry: EmailQueueEntry): Promise<void> {
    await this.collection.updateOne({ _id: entry._id }, { $set: { sent: false } });
  }

  async isEmpty(): Promise<boolean> {
    const count: number = await this.collection.countDocuments({ sent: false, retried: false, retryCount: { $lt: 3 } });
    return count === 0;
  }
}

let emailQueue: EmailQueue;

app.post("/send-email", async (req: Request, res: Response) => {
  const emailData: EmailPayload = req.body;
  await emailQueue.enqueue(emailData);
  res.status(200).json({ message: 'Email added to the queue' });
  sendEmailFromQueue();
});

async function connectToMongo(): Promise<Db> {
  const client: MongoClient = new MongoClient(mongoURI);
  await client.connect();
  return client.db(dbName);
}

async function sendEmailFromQueue(): Promise<void> {
  const emailPayload: EmailQueueEntry | null = await emailQueue.dequeue();
  if (emailPayload) {
    try {
      // Implement your email sending logic here using emailPayload.payload
      console.log('Sending email:', emailPayload.payload);

      // Assume email sending is successful, mark the entry as sent
      await emailQueue.markAsSent(emailPayload);
    } catch (error) {
      // Handle the error, e.g., log it and mark the entry as not sent (for retries)
      console.error('Error sending email:', error);
      await emailQueue.markAsNotSent(emailPayload);
    }

    // Process the next email in the queue
    await sendEmailFromQueue();
  } else {
     // console.log('No more emails in the queue or all retried 3 times');
  }
}

connectToMongo()
  .then((db: Db) => {
    emailQueue = new EmailQueue(db);
    console.log("Connected to MongoDB");
    sendEmailFromQueue();
  })
  .catch((error: Error) => {
    console.error("Error connecting to MongoDB:", error);
  });

app.listen(port, () => {
  console.log(`Message service listening at http://localhost:${port}`);
});
