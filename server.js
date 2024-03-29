const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const excel = require("exceljs");
const {
  SQSClient,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require("@aws-sdk/client-sqs");

const app = express();

const server = http.createServer(app);
const io = new Server(server, {
  serveClient: false,
  cors: {
    origin: "*",
  },
});

// const socketMiddleware = async (socket, next) => {
//   const deviceId = socket.handshake.query.deviceId;
//   if (!deviceId) {
//     return next(new Error("no device id found"));
//   }
//   return next();
// };

// io.use(socketMiddleware);

io.on("connection", (socket) => {
  console.log("connected to socket client");
  const deviceId = socket.handshake.query.deviceId;
  if (deviceId) {
    socket.join(String(deviceId));
    console.log(`Rooms => ${socket.rooms.has(deviceId)}`);
    console.log("joining room => ", deviceId);
  }

  socket.on("receiveQueueMessage", async (data) => {
    console.log(data);
    await pollMessages();
  });
  socket.on("disconnect", () => {
    console.log("socket client disconnected");
  });
});

const client = new SQSClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "AKIAX74RUOGV4SDDR7W3",
    secretAccessKey: "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",
  },
});
app.use(express.json());

function transformData(data) {
  if (!data) {
    return;
  }

  const headers = data[1]; // Ensure headers are an array
  const dataRows = data.slice(2);

  const transformedData = dataRows.map((row) => {
    const object = {};
    for (let i = 1; i < headers.length; i++) {
      if (row && Array.isArray(row)) {
        // Check for null/undefined and array type
        object[`${headers[i]}`] = row[i];
      } else {
        object[`${headers[i]}`] = null; // Assign null for missing values
      }
    }
    return object;
  });

  return transformedData;
}

async function processFileData(data) {
  const workbook = new excel.Workbook();
  await workbook.xlsx.load(data);
  const result = [];
  workbook.eachSheet((sheet, sheetId) => {
    const convertedData = sheet.getSheetValues();
    result.push(transformData(convertedData));
  });
  return result;
}

async function pollMessages() {
  console.log("listening for producer message");
  try {
    const getQueueUrlCommand = new GetQueueUrlCommand({
      QueueName: "file-upload-test-queue",
    });
    const { QueueUrl } = await client.send(getQueueUrlCommand);
    console.log(QueueUrl);
    const recieveMessagCommand = new ReceiveMessageCommand({
      QueueUrl: QueueUrl,
      MaxNumberOfMessages: 1,
    });
    const { Messages } = await client.send(recieveMessagCommand);
    if (Messages && Messages.length > 0) {
      const { Body, ReceiptHandle } = Messages[0];
      // console.log(`Body: ${JSON.stringify(Body)}`)
      const fileData = Buffer.from(JSON.parse(Body).fileData, "base64");
      const deviceId = JSON.parse(Body).deviceId;
      console.log(`Device-id: ${deviceId}`);

      const result = await processFileData(fileData);
      const deleteMessageCommand = new DeleteMessageCommand({
        QueueUrl,
        ReceiptHandle,
      });
      await client.send(deleteMessageCommand);
      return new Promise((resolve) => {
        io.to(deviceId).emit("parse:excel", result, (err, resp) => {
          console.log(`event emitted to room ${deviceId}\n`, result);
          if (err) {
            return resolve({
              error: true,
              message: err?.message ?? "something went wrong",
            });
          }

          return resolve(resp[0] ?? resp);
        });
      });
    }
  } catch (err) {
    console.log(err);
  }
}

const PORT = 5000;
server.listen(PORT, (err) => {
  if (err) {
    console.log(err);
  }
  console.log(`server running on port ${PORT}`);
});

// pollMessages()

// setInterval(pollMessages, 10000);
