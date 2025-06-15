const express = require('express');
const cron = require('node-cron');
const mqtt = require('mqtt');
const sqlite3 = require('sqlite3').verbose();
const dotenv = require('dotenv');
const { open } = require('sqlite');

dotenv.config();

const app = express();
app.use(express.json());

// Initialize SQLite
let db;
async function initializeDatabase() {
  try {
    db = await open({
      filename: './schedules.db',
      driver: sqlite3.Database,
    });
    await db.exec(`
      CREATE TABLE IF NOT EXISTS schedules (
        id TEXT PRIMARY KEY,
        deviceId TEXT,
        roomId TEXT,
        topic TEXT,
        deviceName TEXT,
        action TEXT,
        time INTEGER
      );
      CREATE TABLE IF NOT EXISTS devices (
        deviceId TEXT PRIMARY KEY,
        roomId TEXT,
        topic TEXT,
        deviceName TEXT,
        status TEXT
      );
    `);
    console.log('Database initialized');
  } catch (err) {
    console.error('Error initializing database:', err);
    process.exit(1);
  }
}

// Validate MQTT topic format
function isValidTopic(topic) {
  const topicRegex = /^OnWireWay\/devices\/SONOFF_[A-Z0-9]{6}\/command$/;
  return topicRegex.test(topic);
}

// Initialize MQTT Client
let mqttClient;
function initializeMQTT() {
  const mqttOptions = {
    port: parseInt(process.env.MQTT_PORT) || 1883,
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASSWORD,
    clientId: `server_${Date.now()}`,
    rejectUnauthorized: false, // Set to true if using proper SSL certificates
    reconnectPeriod: 5000,
    connectTimeout: 30000,
  };

  mqttClient = mqtt.connect(process.env.MQTT_BROKER, mqttOptions);

  mqttClient.on('connect', async () => {
    console.log('Connected to MQTT broker');
    try {
      if (db) {
        const devices = await db.all('SELECT topic FROM devices');
        devices.forEach(({ topic }) => {
          if (isValidTopic(topic)) {
            mqttClient.subscribe(topic, (err) => {
              if (err) console.error(`Error subscribing to ${topic}:`, err);
              else console.log(`Subscribed to ${topic}`);
            });
          }
        });
      }
    } catch (err) {
      console.error('Error subscribing to device topics:', err);
    }
  });

  mqttClient.on('message', async (topic, message) => {
    if (isValidTopic(topic)) {
      const status = message.toString().toLowerCase();
      try {
        await db.run('UPDATE devices SET status = ? WHERE topic = ?', [status, topic]);
        console.log(`Updated device status for ${topic}: ${status}`);
      } catch (err) {
        console.error(`Error updating device status for ${topic}:`, err);
      }
    }
  });

  mqttClient.on('error', (err) => {
    console.error('MQTT error:', err);
  });

  mqttClient.on('close', () => {
    console.log('MQTT connection closed');
  });

  mqttClient.on('reconnect', () => {
    console.log('MQTT reconnecting...');
  });
}

// Store active cron jobs
const cronJobs = new Map();

// Load schedules
async function loadSchedules() {
  try {
    const schedules = await db.all('SELECT * FROM schedules');
    console.log(`Loading ${schedules.length} schedules`);
    schedules.forEach((schedule) => {
      // Check if schedule time is in the future
      const scheduleTime = new Date(schedule.time);
      const now = new Date();
      if (scheduleTime > now) {
        scheduleCronJob(schedule.id, schedule);
      } else {
        // Remove expired schedules
        console.log(`Removing expired schedule: ${schedule.id}`);
        db.run('DELETE FROM schedules WHERE id = ?', schedule.id).catch(err => {
          console.error(`Error deleting expired schedule ${schedule.id}:`, err);
        });
      }
    });
  } catch (err) {
    console.error('Error loading schedules:', err);
  }
}

// Schedule a cron job
function scheduleCronJob(scheduleId, schedule) {
  const { time, action, topic } = schedule;
  const scheduleTime = new Date(time);
  const now = new Date();

  // Validate schedule time is in the future
  if (scheduleTime <= now) {
    console.log(`Schedule ${scheduleId} is in the past, skipping`);
    return;
  }

  // Create cron expression for the specific date and time
  const cronExpression = `${scheduleTime.getSeconds()} ${scheduleTime.getMinutes()} ${scheduleTime.getHours()} ${scheduleTime.getDate()} ${scheduleTime.getMonth() + 1} *`;

  console.log(`Scheduling job ${scheduleId} with cron: ${cronExpression}`);

  const job = cron.schedule(cronExpression, async () => {
    try {
      console.log(`Executing schedule ${scheduleId}: ${action} on ${topic}`);
      
      if (!isValidTopic(topic)) {
        throw new Error(`Invalid topic: ${topic}`);
      }

      // Publish MQTT message
      if (mqttClient && mqttClient.connected) {
        mqttClient.publish(topic, action, { qos: 0 }, (err) => {
          if (err) {
            console.error(`Error publishing to ${topic}:`, err);
          } else {
            console.log(`Published ${action} to ${topic}`);
          }
        });
      } else {
        console.error('MQTT client not connected');
      }

      // Update device status in database
      await db.run('UPDATE devices SET status = ? WHERE topic = ?', [action, topic]);
      
      // Remove the executed schedule
      await db.run('DELETE FROM schedules WHERE id = ?', scheduleId);
      
      // Clean up cron job
      cronJobs.delete(scheduleId);
      job.stop();
      
      console.log(`Schedule ${scheduleId} executed and removed`);
    } catch (err) {
      console.error(`Error executing schedule ${scheduleId}:`, err);
    }
  }, {
    scheduled: true,
    timezone: "UTC" // Specify timezone explicitly
  });

  cronJobs.set(scheduleId, job);
  console.log(`Cron job scheduled for ${scheduleId} at ${scheduleTime}`);
}

// API: Register a device
app.post('/devices', async (req, res) => {
  try {
    const { deviceId, roomId, topic, deviceName } = req.body;
    
    if (!deviceId || !roomId || !topic || !deviceName) {
      return res.status(400).json({ error: 'Missing required fields: deviceId, roomId, topic, deviceName' });
    }

    if (!isValidTopic(topic)) {
      return res.status(400).json({ error: 'Invalid topic format' });
    }

    await db.run(
      'INSERT OR REPLACE INTO devices (deviceId, roomId, topic, deviceName, status) VALUES (?, ?, ?, ?, ?)',
      [deviceId, roomId, topic, deviceName, 'off']
    );

    // Subscribe to the device topic
    if (mqttClient && mqttClient.connected) {
      mqttClient.subscribe(topic, (err) => {
        if (err) {
          console.error(`Error subscribing to ${topic}:`, err);
        } else {
          console.log(`Subscribed to ${topic}`);
        }
      });
    }

    res.status(201).json({ message: 'Device registered successfully' });
  } catch (err) {
    console.error('Error registering device:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Create a schedule
app.post('/schedules', async (req, res) => {
  try {
    const { deviceId, roomId, topic, deviceName, action, time } = req.body;
    
    if (!deviceId || !roomId || !topic || !deviceName || !action || !time) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    if (!isValidTopic(topic)) {
      return res.status(400).json({ error: 'Invalid topic format' });
    }

    if (!['on', 'off'].includes(action.toLowerCase())) {
      return res.status(400).json({ error: 'Invalid action: must be "on" or "off"' });
    }

    const scheduleTime = new Date(time);
    const now = new Date();

    if (isNaN(scheduleTime.getTime())) {
      return res.status(400).json({ error: 'Invalid time format' });
    }

    if (scheduleTime <= now) {
      return res.status(400).json({ error: 'Schedule time must be in the future' });
    }

    const scheduleId = `${deviceId}_${Date.now()}`;
    const scheduleTimeMs = scheduleTime.getTime();

    await db.run(
      'INSERT INTO schedules (id, deviceId, roomId, topic, deviceName, action, time) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [scheduleId, deviceId, roomId, topic, deviceName, action.toLowerCase(), scheduleTimeMs]
    );

    scheduleCronJob(scheduleId, { 
      id: scheduleId, 
      deviceId, 
      roomId, 
      topic, 
      deviceName, 
      action: action.toLowerCase(), 
      time: scheduleTimeMs 
    });

    res.status(201).json({ 
      scheduleId,
      message: 'Schedule created successfully',
      scheduledTime: scheduleTime.toISOString()
    });
  } catch (err) {
    console.error('Error creating schedule:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Delete a schedule
app.delete('/schedules/:scheduleId', async (req, res) => {
  try {
    const { scheduleId } = req.params;
    
    const schedule = await db.get('SELECT * FROM schedules WHERE id = ?', scheduleId);
    if (!schedule) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    await db.run('DELETE FROM schedules WHERE id = ?', scheduleId);
    
    const job = cronJobs.get(scheduleId);
    if (job) {
      job.stop();
      cronJobs.delete(scheduleId);
    }

    res.status(200).json({ message: 'Schedule deleted successfully' });
  } catch (err) {
    console.error('Error deleting schedule:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get schedules for a device
app.get('/schedules/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const schedules = await db.all('SELECT * FROM schedules WHERE deviceId = ?', deviceId);
    
    // Convert timestamps to readable dates
    const formattedSchedules = schedules.map(schedule => ({
      ...schedule,
      scheduledTime: new Date(schedule.time).toISOString()
    }));

    res.status(200).json(formattedSchedules);
  } catch (err) {
    console.error('Error fetching schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get all schedules
app.get('/schedules', async (req, res) => {
  try {
    const schedules = await db.all('SELECT * FROM schedules ORDER BY time ASC');
    
    const formattedSchedules = schedules.map(schedule => ({
      ...schedule,
      scheduledTime: new Date(schedule.time).toISOString()
    }));

    res.status(200).json(formattedSchedules);
  } catch (err) {
    console.error('Error fetching all schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get device status
app.get('/devices/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const device = await db.get('SELECT * FROM devices WHERE deviceId = ?', deviceId);
    
    if (!device) {
      return res.status(404).json({ error: 'Device not found' });
    }

    res.status(200).json(device);
  } catch (err) {
    console.error('Error fetching device:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get all devices
app.get('/devices', async (req, res) => {
  try {
    const devices = await db.all('SELECT * FROM devices ORDER BY deviceName ASC');
    res.status(200).json(devices);
  } catch (err) {
    console.error('Error fetching devices:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  const status = {
    server: 'running',
    database: db ? 'connected' : 'disconnected',
    mqtt: mqttClient && mqttClient.connected ? 'connected' : 'disconnected',
    activeSchedules: cronJobs.size
  };
  res.status(200).json(status);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  
  // Stop all cron jobs
  cronJobs.forEach((job, scheduleId) => {
    job.stop();
    console.log(`Stopped cron job: ${scheduleId}`);
  });
  cronJobs.clear();

  // Close MQTT connection
  if (mqttClient) {
    mqttClient.end();
  }

  // Close database connection
  if (db) {
    await db.close();
  }

  process.exit(0);
});

const PORT = process.env.PORT || 3000;

// Initialize everything in the correct order
async function startServer() {
  try {
    await initializeDatabase();
    initializeMQTT();
    await loadSchedules();
    
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Health check available at http://localhost:${PORT}/health`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();