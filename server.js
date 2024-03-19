const express = require('express');
const bunyan = require('bunyan');
const app = express();
app.use(express.json());
const fs = require('fs');
const path = require('path');
const reqLogFilePath = path.join(__dirname, 'logs', 'requests.log');
const todosLogFilePath = path.join(__dirname, 'logs', 'todos.log');
const { performance } = require('perf_hooks');
const { Client } = require('pg');
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');

////////////////////////////////////Creating new files////////////////////////////////////
fs.writeFile(reqLogFilePath, '', (err) => {
  if (err) {
    console.error('Error writing to log file:', err);
  }
});

fs.writeFile(todosLogFilePath, '', (err) => {
  if (err) {
    console.error('Error writing to log file:', err);
  }
});

////////////////////////////////////Configuring Loggers' Format////////////////////////////////////
class requestsLoggerStream {
  write(record) {
    const logData = JSON.parse(record);
    const logLevel = bunyan.nameFromLevel[logData.level];
    const logMessage = logData.msg;
    const requestNumber = logData.reqId;

    const currentDate = new Date();
    const day = String(currentDate.getDate()).padStart(2, '0');
    const month = String(currentDate.getMonth() + 1).padStart(2, '0');
    const year = currentDate.getFullYear();
    const hours = String(currentDate.getHours()).padStart(2, '0');
    const minutes = String(currentDate.getMinutes()).padStart(2, '0');
    const seconds = String(currentDate.getSeconds()).padStart(2, '0');
    const milliseconds = String(currentDate.getMilliseconds()).padStart(3, '0');

    const formattedDate = `${day}-${month}-${year} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    const formattedLog = `${formattedDate} ${logLevel.toUpperCase()}: ${logMessage} | request #${requestNumber} `;

    console.log(formattedLog);

    fs.appendFile(reqLogFilePath, formattedLog + '\n', (err) => {
      if (err) {
        console.error('Error writing to log file:', err);
      }
    });
  }
}

class todosLoggerStream {
  write(record) {
    const logData = JSON.parse(record);
    const logLevel = bunyan.nameFromLevel[logData.level];
    const logMessage = logData.msg;
    const requestNumber = logData.reqId;

    const currentDate = new Date();
    const day = String(currentDate.getDate()).padStart(2, '0');
    const month = String(currentDate.getMonth() + 1).padStart(2, '0');
    const year = currentDate.getFullYear();
    const hours = String(currentDate.getHours()).padStart(2, '0');
    const minutes = String(currentDate.getMinutes()).padStart(2, '0');
    const seconds = String(currentDate.getSeconds()).padStart(2, '0');
    const milliseconds = String(currentDate.getMilliseconds()).padStart(3, '0');

    const formattedDate = `${day}-${month}-${year} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    const formattedLog = `${formattedDate} ${logLevel.toUpperCase()}: ${logMessage} | request #${requestNumber} `;

    console.log(formattedLog);

    fs.appendFile(todosLogFilePath, formattedLog + '\n', (err) => {
      if (err) {
        console.error('Error writing to log file:', err);
      }
    });
  }
}

////////////////////////////////////Loggers Configuration////////////////////////////////////
const requestsLogger = bunyan.createLogger({
  name: 'request-logger',
  level: 'info',
  stream: new requestsLoggerStream()
});

const todosLogger = bunyan.createLogger({
  name: 'todo-logger',
  level: 'info',
  stream: new todosLoggerStream()
});

////////////////////////////////////Global Variables////////////////////////////////////
let logsCounter = 1, startTime, endTime, duration;

const validStatusValues = ["ALL", "PENDING", "LATE", "DONE"];
const validSortByValues = ["ID", "DUE_DATE", "TITLE"];

const sortToColumnsMapping = new Map([
  ["ID", "rawid"],
  ["TITLE", "title"],
  ["DUE_DATE", "duedate"]
]);

const sizeQuery = {
  text: "SELECT * FROM todos"
};

const port = 9285;

////////////////////////////////////Starting the Server////////////////////////////////////
app.listen(port, () => {
  console.log('Hello World! Server is listening on port', port);
});

////////////////////////////////////DBs configurations////////////////////////////////////
const dbConfig = {
  user: 'postgres',
  password: 'docker',
  host: 'postgres', // Docker container name
  port: 5432,        // Default PostgreSQL port
  database: 'todos',
};

const pgClient = new Pool(dbConfig);

const mongoClient = new MongoClient('mongodb://mongo/todos', { useNewUrlParser: true, useUnifiedTopology: true })
mongoClient.connect()
  .then(() => console.log('Connected to local MongoDB'))
  .catch(err => console.error('Error connecting to local MongoDB', err));
const mongoCollection = mongoClient.db().collection('todos');

////////////////////////////////////GET /todo/health endpoint////////////////////////////////////
app.get('/todo/health', (req, res) => {
  startTime = performance.now();

  requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo/health | HTTP Verb GET");
  res.status(200).send("OK");
  logDebbugerMode(startTime);
});

////////////////////////////////////POST /todo method////////////////////////////////////
app.post('/todo', (req, res) => {
  startTime = performance.now();

  requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo | HTTP Verb POST");
  let currDate = new Date(), requestTitle = req.body.title;

  const selectQuery = {
    text: 'SELECT * FROM todos WHERE title = $1',
    values: [requestTitle],
  };

  pgClient.query(selectQuery).then(result => {
    if (result.rows.length > 0) {
      todosLogger.error({reqId: logsCounter}, "Error: TODO with the title [" + req.body.title +  "] already exists in the system");
      res.status(409).json({ result: '', errorMessage: "Error: TODO with the title [" + req.body.title +  "] already exists in the system" });
      logDebbugerMode(startTime);
    } else if (currDate.getTime() > req.body.dueDate) {
      todosLogger.error({reqId: logsCounter}, "Error: Can't create new TODO that its due date is in the past");
      res.status(409).json({ result: '', errorMessage: "Error: Can't create new TODO that its due date is in the past" });
      logDebbugerMode(startTime);
    } else {
      todosLogger.info({reqId: logsCounter}, "Creating new TODO with Title [" + req.body.title + "]");
      todosLogger.debug({reqId: logsCounter}, `Currently there are ${result.rows.length} 
      TODOs in the system. New TODO will be assigned with id ${result.rows.length + 1}`);

      pgClient.query(sizeQuery).then(result => {
        const newTODO = {
          rawid: result.rows.length + 1,
          title: req.body.title,
          content: req.body.content,
          duedate: req.body.dueDate,
          state: "PENDING"
        };

        const insertQuery = {
          text: 'INSERT INTO todos (rawid, title, content, duedate, state) VALUES ($1, $2, $3, $4, $5)',
          values: [newTODO.rawid, newTODO.title, newTODO.content, newTODO.duedate, newTODO.state],
        };

        pgClient.query(insertQuery);
        mongoCollection.insertOne(newTODO);

        res.status(200).json({ result: result.rows.length + 1, errorMessage: "" });
        logDebbugerMode(startTime);
      });
    }
  });
});

////////////////////////////////////GET /todo/size method////////////////////////////////////
app.get('/todo/size', (req, res) => {
  startTime = performance.now();  
  let status = req.query.status;

  if (!validStatusValues.includes(status)) {
    res.status(400).json({ result: '', errorMessage: "Bad Request" });
  } else {
    requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo/size | HTTP Verb GET");

    if (req.query.persistenceMethod === "POSTGRES") {
      let pgQuery;

      if (status === "ALL") {
        pgQuery = {
          text: "SELECT * FROM todos"
        };
      } else {
        pgQuery = {
          text: "SELECT * FROM todos WHERE state = $1",
          values: [status]
        };
      }
  
      pgClient.query(pgQuery).then(result => {
        todosLogger.info({reqId: logsCounter}, `Total TODOs count for state ${status} is ${result.rows.length}`);
        res.status(200).json({result: result.rows.length, errorMessage: ""});
        logDebbugerMode(startTime);
      });
    } else {
      getTodosCountByStatus(status, res);
      logDebbugerMode(startTime);
    }
  }
});

////////////////////////////////////GET /todo/content method////////////////////////////////////
app.get('/todo/content', (req, res) => {
  startTime = performance.now();
  let status = req.query.status, sortBy = req.query.sortBy;

  if (!sortBy) {
    sortBy = "ID";
  }

  if (!validStatusValues.includes(status) || !validSortByValues.includes(sortBy)) {
      res.status(400).json({ result: '', errorMessage: "Bad Request" });
  } else {
    requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo/content | HTTP Verb GET");
    todosLogger.info({reqId: logsCounter}, `Extracting todos content. Filter: ${status} | Sorting by: ${sortBy}`);

    if (req.query.persistenceMethod === "POSTGRES") {
      let queryInfo;

      if (status === "ALL") {
        queryInfo = {
          text: `SELECT * FROM todos ORDER BY ${sortToColumnsMapping.get(sortBy)}`
        };
      } else {
        queryInfo = {
          text: `SELECT * FROM todos WHERE state = $1 ORDER BY ${sortToColumnsMapping.get(sortBy)}`,
          values: [status]
        };
      }
  
      pgClient.query(queryInfo).then(result => {
        pgClient.query(sizeQuery).then(sizeResult => {
          todosLogger.debug({reqId: logsCounter}, `There are a total of ${sizeResult.rows.length} todos in the system. The result holds ${result.rows.length} todos`);
          let todosArr = [];
    
          for (let i = 0; i < result.rows.length; i++) {
            todosArr[i] = {
              id: result.rows[i].rawid,
              title: result.rows[i].title,
              content: result.rows[i].content,
              status: result.rows[i].state,
              dueDate: parseInt(result.rows[i].duedate)
            }
          }
    
          res.status(200).json({result: todosArr, errorMessage: ""});
          logDebbugerMode(startTime);
        })
      });
    } else {
      getSortedTodosByStatus(status, sortBy, res);
      logDebbugerMode(startTime);
    }
  }
});

////////////////////////////////////PUT /todo method////////////////////////////////////
app.put('/todo', (req, res) => {
  startTime = performance.now();
  let param_id = req.query.id, param_status = req.query.status;

  if (!validStatusValues.includes(param_status)) {
    res.status(400).json({result:"", errorMessage: "Bad Request"});
  } else {
    requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo | HTTP Verb PUT");
    let oldStatus;

    const updateQuery = {
      text: "UPDATE todos SET state = $1 WHERE rawid = $2",
      values: [param_status, param_id]
    };

    const selectQuery = {
      text: "SELECT state FROM todos WHERE rawid = $1",
      values: [param_id]
    };

    pgClient.query(selectQuery).then(result => {
      if (result.rows.length < 1) {
        todosLogger.error({reqId: logsCounter++}, `Error: no such TODO with id ${param_id}`);
        res.status(404).json({result: "", errorMessage: `Error: no such TODO with id ${param_id}`});
      } else {
        oldStatus = result.rows[0].state;

        pgClient.query(updateQuery);
        updateTODOStatusInMongo(parseInt(param_id), param_status);

        todosLogger.info({reqId: logsCounter}, `Update TODO id [${param_id}] state to ${param_status}`);
        todosLogger.debug({reqId: logsCounter}, `Todo id [${param_id}] state change: ${oldStatus} --> ${param_status}`);
    
        res.json({result: oldStatus, errorMessage: ""});
        logDebbugerMode(startTime);
      }
    });
  }
});

async function updateTODOStatusInMongo(id, status) {
  try {
    const updatedDocument = await mongoCollection.findOneAndUpdate(
      { rawid: id },
      { $set: { state: status } },
      { returnDocument: 'after' }
    );
  } catch (error) {
    console.error('Error updating TODO status:', error);
    throw error;
  }
}

////////////////////////////////////DELETE /todo method////////////////////////////////////
app.delete('/todo', (req, res) => {
  startTime = performance.now();
  requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /todo | HTTP Verb DELETE");
  let param_id = req.query.id;

  pgClient.query(sizeQuery).then(result => {
    if (result.rows.length < 1) {
      todosLogger.error({reqId: logsCounter++}, `Error: no such TODO with id ${param_id}`);
      res.status(404).json({result:"", errorMessage:`Error: no such TODO with id ${param_id}`});
      logDebbugerMode(startTime);
    } else {
      todosLogger.info({reqId: logsCounter}, `Removing todo id ${param_id}`);
      const deleteQuery = {
        text: "DELETE FROM todos WHERE rawid = $1",
        values: [param_id]
      };
  
      mongoCollection.deleteOne({ rawid: parseInt(param_id) });
  
      pgClient.query(deleteQuery).then(() => {
        pgClient.query(sizeQuery).then(result => {
          todosLogger.debug({reqId: logsCounter}, `After removing todo id [${param_id}] there are ${result.rows.length} TODOs in the system`);
          res.status(200).json({result: result.rows.length, errorMessage: ""});
          logDebbugerMode(startTime);
        });
      });
    }
  });
});

////////////////////////////////////GET /logs/level method////////////////////////////////////
app.get('/logs/level', (req, res) => {
  startTime = performance.now();
  let loggerName = req.query['logger-name'], resLevel;

  if (loggerName != "request-logger" && loggerName != "todo-logger") {
    res.status(400).send(`Bad Request: No such logger named ${loggerName}`);
  } else {
    requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter +" | resource: /logs/level | HTTP Verb GET");

    if (loggerName == "request-logger") {
      resLevel = requestsLogger.level();
    } else {
      resLevel = todosLogger.level();
    }

    res.status(200).send(resLevel.toUpperCase());
  }

  endTime = performance.now();
  duration = endTime - startTime;
  requestsLogger.debug({reqId: logsCounter}, "request #" + logsCounter + " duration: " + duration + "ms");
  logsCounter++;
});

////////////////////////////////////PUT /logs/level method////////////////////////////////////
app.put('/logs/level', (req, res) => {
  startTime = performance.now();
  let loggerName = req.query['logger-name'], desiredLevel = req.query['logger-level'];

  if (loggerName != "request-logger" && loggerName != "todo-logger") {
    res.status(400).send(`Bad Request: No such logger named ${loggerName}`);
  } else if (desiredLevel != "INFO" && desiredLevel != "DEBUG" && desiredLevel != "ERROR") {
    res.status(400).send(`Bad Request: Invalid level ${desiredLevel}`);
  } else {
    requestsLogger.info({reqId: logsCounter}, "Incoming request | #" + logsCounter + " | resource: /logs/level | HTTP Verb PUT");
    
    if (loggerName == "request-logger") {
      resLevel = requestsLogger.level(desiredLevel);
    } else {
      resLevel = todosLogger.level(desiredLevel);
    }

    res.status(200).send(desiredLevel);
  }

  endTime = performance.now();
  duration = endTime - startTime;
  requestsLogger.debug({reqId: logsCounter}, "request #" + logsCounter + " duration: " + duration + "ms");
  logsCounter++;
});

////////////////////////////////////Methods////////////////////////////////////
async function getTodosCountByStatus(status, res) {
  try {
    let queryResult;

    if (status === "ALL") {
      queryResult = await mongoCollection.find({}).toArray();
    } else {
      queryResult = await mongoCollection.find({ state: status }).toArray();
    }

    const todosCount = queryResult.length;
    todosLogger.info({ reqId: logsCounter }, `Total TODOs count for state ${status} is ${todosCount}`);
    res.status(200).json({ result: todosCount, errorMessage: "" });    
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ result: null, errorMessage: 'Internal Server Error' });
  }
}

async function getSortedTodosByStatus(status, sortBy, res) {
  try {
    let queryResult, parsedArr = [], sortQuery;

    if (sortBy === "TITLE") {
      sortQuery = {title: 1};
    } else if (sortBy === "DUE_DATE") {
      sortQuery = {duedate: 1};
    } else {
      sortQuery = {rawid: 1};
    }

    if (status === "ALL") {
      queryResult = await mongoCollection.find({}).sort(sortQuery).toArray();
    } else {
      queryResult = await mongoCollection.find({state: status}).sort(sortQuery).toArray();
    }

    for (let i = 0; i < queryResult.length; i++) {
      parsedArr[i] = {
        id: queryResult[i].rawid,
        title: queryResult[i].title,
        content: queryResult[i].content,
        status: queryResult[i].state,
        dueDate: parseInt(queryResult[i].duedate)
      }
    }

    res.status(200).json({result: parsedArr, errorMessage: ""});  
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ result: null, errorMessage: 'Internal Server Error' });
  }
}

async function logDebbugerMode(startTime) {
  endTime = performance.now();
  duration = endTime - startTime;
  requestsLogger.debug({reqId: logsCounter}, "request #" + logsCounter + " duration: " + duration + "ms");
  logsCounter++;
}