const express = require("express");
const http = require("http");
const {Server} = require("socket.io")
const excel = require("exceljs")
const {
  SQSClient,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand
} = require("@aws-sdk/client-sqs");

const app = express();

const server = http.createServer(app);
const io = new Server(server, {
  serveClient: false,
  cors: {
    origin: "*"
  },
});

io.on("connection", (socket) => {
  console.log("connected to socket client");
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
        if (row && Array.isArray(row)) {  // Check for null/undefined and array type
          object[`${headers[i]}`] = row[i];
        } else {
          object[`${headers[i]}`] = null;  // Assign null for missing values
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
    console.log("listening for producer message")
  try {
    while (true) {
      const getQueueUrlCommand = new GetQueueUrlCommand({
        QueueName: "file-upload-test-queue",
      });
      const { QueueUrl } = await client.send(getQueueUrlCommand);
      const recieveMessagCommand = new ReceiveMessageCommand({
        QueueUrl: QueueUrl,
      });
      const {Messages} = await client.send(recieveMessagCommand);
      if(Messages && Messages.length > 0){
        const {Body, ReceiptHandle} = Messages[0];
        const fileData = Buffer.from(JSON.parse(Body).fileData, "base64")
        const result = await processFileData(fileData);
        const deleteMessageCommand = new DeleteMessageCommand({QueueUrl,ReceiptHandle});
        await client.send(deleteMessageCommand);
        return new Promise((resolve) => {
          io.timeout(10000).emit('parse:excel', result, (err, resp) => {
            if (err) {
              return resolve({
                error: true,
                message: err?.message ?? 'something went wrong'
              });
            }
    
            return resolve(resp[0] ?? resp);
          });
        })
      }
      
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

pollMessages()
