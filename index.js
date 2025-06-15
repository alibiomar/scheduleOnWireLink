const express = require('express');
const cron = require('node-cron');
const mqtt = require('mqtt');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
app.use(express.json());

// In-memory storage for Render compatibility
let devices = new Map();
let schedules = new Map();

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
    rejectUnauthorized: false,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    keepalive: 60,
  };

  mqttClient = mqtt.connect(process.env.MQTT_BROKER, mqttOptions);

  mqttClient.on('connect', async () => {
    console.log('Connected to MQTT broker');
    try {
      // Subscribe to all registered device topics
      devices.forEach((device) => {
        if (isValidTopic(device.topic)) {
          mqttClient.subscribe(device.topic, (err) => {
            if (err) console.error(`Error subscribing to ${device.topic}:`, err);
            else console.log(`Subscribed to ${device.topic}`);
          });
        }
      });
    } catch (err) {
      console.error('Error subscribing to device topics:', err);
    }
  });

  mqttClient.on('message', async (topic, message) => {
    if (isValidTopic(topic)) {
      const status = message.toString().toLowerCase();
      try {
        // Update device status in memory
        devices.forEach((device, deviceId) => {
          if (device.topic === topic) {
            device.status = status;
            console.log(`Updated device status for ${topic}: ${status}`);
          }
        });
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

// Store active cron jobs and timeouts
const cronJobs = new Map();
const scheduleTimeouts = new Map();

// Schedule execution using setTimeout (more reliable on Render)
function scheduleExecution(scheduleId, schedule) {
  const { time, action, topic } = schedule;
  const scheduleTime = new Date(time);
  const now = new Date();

  console.log(`Processing schedule ${scheduleId}:`);
  console.log(`  Current time: ${now.toISOString()}`);
  console.log(`  Schedule time: ${scheduleTime.toISOString()}`);
  console.log(`  Action: ${action} on ${topic}`);

  // Validate schedule time is in the future
  if (scheduleTime <= now) {
    console.log(`Schedule ${scheduleId} is in the past (${scheduleTime.toISOString()} <= ${now.toISOString()}), removing`);
    schedules.delete(scheduleId);
    return;
  }

  const delay = scheduleTime.getTime() - now.getTime();
  console.log(`Schedule ${scheduleId} will execute in ${Math.round(delay / 1000)} seconds (${Math.round(delay / 60000)} minutes)`);
  
  // Use setTimeout for schedules (more reliable than cron on cloud platforms)
  const timeoutId = setTimeout(async () => {
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
            console.log(`Successfully published ${action} to ${topic}`);
          }
        });
      } else {
        console.error('MQTT client not connected, cannot publish message');
      }

      // Update device status in memory
      devices.forEach((device, deviceId) => {
        if (device.topic === topic) {
          device.status = action;
          console.log(`Updated device ${deviceId} status to ${action}`);
        }
      });
      
      // Remove the executed schedule
      schedules.delete(scheduleId);
      scheduleTimeouts.delete(scheduleId);
      
      console.log(`Schedule ${scheduleId} executed and removed successfully`);
    } catch (err) {
      console.error(`Error executing schedule ${scheduleId}:`, err);
    }
  }, delay);

  scheduleTimeouts.set(scheduleId, timeoutId);
  console.log(`Schedule ${scheduleId} set for ${scheduleTime.toISOString()} using setTimeout`);
}

// Cleanup expired schedules periodically
function cleanupExpiredSchedules() {
  const now = new Date();
  const expired = [];
  
  schedules.forEach((schedule, scheduleId) => {
    if (new Date(schedule.time) <= now) {
      expired.push(scheduleId);
    }
  });
  
  expired.forEach(scheduleId => {
    console.log(`Removing expired schedule: ${scheduleId}`);
    schedules.delete(scheduleId);
    
    // Clean up associated jobs/timeouts
    const job = cronJobs.get(scheduleId);
    if (job) {
      job.stop();
      cronJobs.delete(scheduleId);
    }
    
    const timeoutId = scheduleTimeouts.get(scheduleId);
    if (timeoutId) {
      clearTimeout(timeoutId);
      scheduleTimeouts.delete(scheduleId);
    }
  });
}

// Clean up expired schedules every hour
setInterval(cleanupExpiredSchedules, 60 * 60 * 1000);

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

    const device = {
      deviceId,
      roomId,
      topic,
      deviceName,
      status: 'off'
    };

    devices.set(deviceId, device);

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
    const { deviceId, roomId, topic, deviceName, action, time, timezone } = req.body;
    
    if (!deviceId || !roomId || !topic || !deviceName || !action || !time) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    if (!isValidTopic(topic)) {
      return res.status(400).json({ error: 'Invalid topic format' });
    }

    if (!['on', 'off'].includes(action.toLowerCase())) {
      return res.status(400).json({ error: 'Invalid action: must be "on" or "off"' });
    }

    // Parse the time - handle both ISO strings and timestamps
    let scheduleTime;
    if (typeof time === 'string') {
      scheduleTime = new Date(time);
    } else {
      scheduleTime = new Date(time);
    }

    // If timezone offset is provided, adjust the time
    if (timezone !== undefined) {
      const offsetMs = timezone * 60 * 1000;
      scheduleTime = new Date(scheduleTime.getTime() - offsetMs);
    }

    const now = new Date();

    console.log('Creating schedule:');
    console.log(`  Current time (UTC): ${now.toISOString()}`);
    console.log(`  Original time: ${time}`);
    console.log(`  Parsed schedule time (UTC): ${scheduleTime.toISOString()}`);
    console.log(`  Timezone offset: ${timezone || 'none'} minutes`);
    console.log(`  Time difference: ${scheduleTime.getTime() - now.getTime()}ms`);
    console.log(`  Minutes until execution: ${Math.round((scheduleTime.getTime() - now.getTime()) / 60000)}`);

    if (isNaN(scheduleTime.getTime())) {
      return res.status(400).json({ error: 'Invalid time format' });
    }

    // Add a small buffer (10 seconds) to account for processing time
    const minimumFutureTime = new Date(now.getTime() + 10000);
    
    if (scheduleTime <= minimumFutureTime) {
      console.log(`Schedule time ${scheduleTime.toISOString()} is not sufficiently in the future (minimum: ${minimumFutureTime.toISOString()})`);
      return res.status(400).json({ 
        error: 'Schedule time must be at least 10 seconds in the future',
        currentTime: now.toISOString(),
        scheduleTime: scheduleTime.toISOString(),
        minimumTime: minimumFutureTime.toISOString()
      });
    }

    const scheduleId = `${deviceId}_${Date.now()}`;
    const schedule = {
      id: scheduleId,
      deviceId,
      roomId,
      topic,
      deviceName,
      action: action.toLowerCase(),
      time: scheduleTime.getTime()
    };

    schedules.set(scheduleId, schedule);
    console.log(`Schedule stored: ${JSON.stringify(schedule)}`);
    
    scheduleExecution(scheduleId, schedule);

    res.status(201).json({ 
      scheduleId,
      message: 'Schedule created successfully',
      scheduledTime: scheduleTime.toISOString(),
      currentTime: now.toISOString(),
      delayMinutes: Math.round((scheduleTime.getTime() - now.getTime()) / 60000),
      originalTime: time,
      timezoneOffset: timezone || 0
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
    
    const schedule = schedules.get(scheduleId);
    if (!schedule) {
      return res.status(404).json({ error: 'Schedule not found' });
    }

    schedules.delete(scheduleId);
    
    // Clean up cron job
    const job = cronJobs.get(scheduleId);
    if (job) {
      job.stop();
      cronJobs.delete(scheduleId);
    }
    
    // Clean up timeout
    const timeoutId = scheduleTimeouts.get(scheduleId);
    if (timeoutId) {
      clearTimeout(timeoutId);
      scheduleTimeouts.delete(scheduleId);
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
    const deviceSchedules = [];
    
    schedules.forEach((schedule) => {
      if (schedule.deviceId === deviceId) {
        deviceSchedules.push({
          ...schedule,
          scheduledTime: new Date(schedule.time).toISOString()
        });
      }
    });

    res.status(200).json(deviceSchedules);
  } catch (err) {
    console.error('Error fetching schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get all schedules
app.get('/schedules', async (req, res) => {
  try {
    const allSchedules = Array.from(schedules.values())
      .map(schedule => ({
        ...schedule,
        scheduledTime: new Date(schedule.time).toISOString()
      }))
      .sort((a, b) => a.time - b.time);

    res.status(200).json(allSchedules);
  } catch (err) {
    console.error('Error fetching all schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get device status
app.get('/devices/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const device = devices.get(deviceId);
    
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
    const allDevices = Array.from(devices.values())
      .sort((a, b) => a.deviceName.localeCompare(b.deviceName));
    
    res.status(200).json(allDevices);
  } catch (err) {
    console.error('Error fetching devices:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Manual device control
app.post('/devices/:deviceId/control', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { action } = req.body;
    
    if (!['on', 'off'].includes(action?.toLowerCase())) {
      return res.status(400).json({ error: 'Invalid action: must be "on" or "off"' });
    }
    
    const device = devices.get(deviceId);
    if (!device) {
      return res.status(404).json({ error: 'Device not found' });
    }

    const normalizedAction = action.toLowerCase();
    
    // Publish MQTT message
    if (mqttClient && mqttClient.connected) {
      mqttClient.publish(device.topic, normalizedAction, { qos: 0 }, (err) => {
        if (err) {
          console.error(`Error publishing to ${device.topic}:`, err);
          return res.status(500).json({ error: 'Failed to send command' });
        } else {
          console.log(`Published ${normalizedAction} to ${device.topic}`);
          // Update device status
          device.status = normalizedAction;
          res.status(200).json({ 
            message: 'Command sent successfully', 
            device: device 
          });
        }
      });
    } else {
      res.status(503).json({ error: 'MQTT client not connected' });
    }
  } catch (err) {
    console.error('Error controlling device:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  const now = new Date();
  const schedulesArray = Array.from(schedules.values());
  const upcomingSchedules = schedulesArray.map(s => ({
    id: s.id,
    deviceName: s.deviceName,
    action: s.action,
    scheduledTime: new Date(s.time).toISOString(),
    minutesUntilExecution: Math.round((s.time - now.getTime()) / 60000)
  }));

  const status = {
    server: 'running',
    mqtt: mqttClient && mqttClient.connected ? 'connected' : 'disconnected',
    devices: devices.size,
    activeSchedules: schedules.size,
    timeouts: scheduleTimeouts.size,
    uptime: Math.round(process.uptime()),
    currentTime: now.toISOString(),
    upcomingSchedules: upcomingSchedules.sort((a, b) => a.minutesUntilExecution - b.minutesUntilExecution)
  };
  res.status(200).json(status);
});

// Keep-alive endpoint for Render
app.get('/ping', (req, res) => {
  res.status(200).json({ message: 'pong', timestamp: new Date().toISOString() });
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
  
  // Clear all timeouts
  scheduleTimeouts.forEach((timeoutId, scheduleId) => {
    clearTimeout(timeoutId);
    console.log(`Cleared timeout: ${scheduleId}`);
  });
  scheduleTimeouts.clear();

  // Close MQTT connection
  if (mqttClient) {
    mqttClient.end();
  }

  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

const PORT = process.env.PORT || 3000;

// Initialize everything
function startServer() {
  try {
    initializeMQTT();
    
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Health check available at http://localhost:${PORT}/health`);
      console.log(`Ping endpoint available at http://localhost:${PORT}/ping`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();