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

// Universal timezone utility functions
function parseTimeWithTimezone(timeInput, timezoneOffset) {
  let parsedTime;
  
  // Handle various time input formats
  if (typeof timeInput === 'string') {
    // If it's an ISO string with timezone info, use it directly
    if (timeInput.includes('T') && (timeInput.includes('Z') || timeInput.includes('+') || timeInput.includes('-'))) {
      parsedTime = new Date(timeInput);
    } else {
      // For strings without timezone info, we need to be more explicit
      // Check if it looks like an ISO string without timezone
      if (timeInput.includes('T')) {
        // If no timezone offset is provided, treat the time as if it's already in the client's intended timezone
        // This means the client sent their local time, so we parse it as UTC first
        parsedTime = new Date(timeInput.endsWith('Z') ? timeInput : timeInput + 'Z');
        
        // If timezone offset is provided, we need to adjust from the client's timezone to UTC
        if (typeof timezoneOffset === 'number') {
          // The client sent their local time, so we need to convert it to UTC
          // timezoneOffset is the client's offset from UTC in minutes
          const offsetMs = timezoneOffset * 60 * 1000;
          parsedTime = new Date(parsedTime.getTime() - offsetMs);
        }
      } else {
        // Regular date string parsing
        parsedTime = new Date(timeInput);
      }
    }
  } else if (typeof timeInput === 'number') {
    // Treat as timestamp
    parsedTime = new Date(timeInput);
  } else {
    throw new Error('Invalid time format');
  }

  return parsedTime;
}

function formatTimeForResponse(utcTimestamp, clientTimezoneOffset) {
  const utcTime = new Date(utcTimestamp);
  
  // If client timezone offset is provided, show time in client's timezone
  if (typeof clientTimezoneOffset === 'number') {
    const localTime = new Date(utcTime.getTime() + (clientTimezoneOffset * 60 * 1000));
    return {
      utc: utcTime.toISOString(),
      local: localTime.toISOString(),
      timezone: `UTC${clientTimezoneOffset >= 0 ? '+' : ''}${Math.floor(clientTimezoneOffset / 60)}:${Math.abs(clientTimezoneOffset % 60).toString().padStart(2, '0')}`
    };
  }
  
  return {
    utc: utcTime.toISOString(),
    local: utcTime.toISOString(),
    timezone: 'UTC'
  };
}

function getCurrentUTCTime() {
  return new Date();
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
  const now = getCurrentUTCTime();

  console.log(`Processing schedule ${scheduleId}:`);
  console.log(`  Current UTC time: ${now.toISOString()}`);
  console.log(`  Schedule UTC time: ${scheduleTime.toISOString()}`);
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
  const now = getCurrentUTCTime();
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



// API: Create a schedule with universal timezone support
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

    // Parse the time with universal timezone support
    let scheduleTime;
    try {
      scheduleTime = parseTimeWithTimezone(time, timezone);
    } catch (err) {
      return res.status(400).json({ 
        error: 'Invalid time format',
        details: err.message,
        expectedFormats: [
          'ISO string with timezone: "2024-01-15T14:30:00Z"',
          'ISO string with offset: "2024-01-15T14:30:00+05:00"',
          'Local time string with timezone offset: "2024-01-15T14:30:00" + timezone: -300',
          'Unix timestamp: 1705315800000'
        ]
      });
    }

    const now = getCurrentUTCTime();

    console.log('Creating schedule with universal timezone support:');
    console.log(`  Current UTC time: ${now.toISOString()}`);
    console.log(`  Original time input: ${time}`);
    console.log(`  Client timezone offset: ${timezone !== undefined ? timezone : 'not provided'} minutes`);
    console.log(`  Parsed schedule time (UTC): ${scheduleTime.toISOString()}`);
    console.log(`  Time difference: ${scheduleTime.getTime() - now.getTime()}ms`);
    console.log(`  Minutes until execution: ${Math.round((scheduleTime.getTime() - now.getTime()) / 60000)}`);
    
    // Additional debugging for timezone issues
    if (typeof timezone === 'number') {
      const clientLocalTime = new Date(scheduleTime.getTime() + (timezone * 60 * 1000));
      console.log(`  Client's local time would be: ${clientLocalTime.toISOString()}`);
    }

    if (isNaN(scheduleTime.getTime())) {
      return res.status(400).json({ error: 'Invalid time format - could not parse date' });
    }

    // Add a small buffer (10 seconds) to account for processing time
    const minimumFutureTime = new Date(now.getTime() + 10000);
    
    if (scheduleTime <= minimumFutureTime) {
      console.log(`Schedule time ${scheduleTime.toISOString()} is not sufficiently in the future (minimum: ${minimumFutureTime.toISOString()})`);
      return res.status(400).json({ 
        error: 'Schedule time must be at least 10 seconds in the future',
        times: {
          current: formatTimeForResponse(now.getTime(), timezone),
          requested: formatTimeForResponse(scheduleTime.getTime(), timezone),
          minimum: formatTimeForResponse(minimumFutureTime.getTime(), timezone)
        }
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
      time: scheduleTime.getTime(), // Store as UTC timestamp
      originalTimezone: timezone // Store original timezone for reference
    };

    schedules.set(scheduleId, schedule);
    console.log(`Schedule stored: ${JSON.stringify({...schedule, time: new Date(schedule.time).toISOString()})}`);
    
    scheduleExecution(scheduleId, schedule);

    const responseTime = formatTimeForResponse(scheduleTime.getTime(), timezone);
    res.status(201).json({ 
      scheduleId,
      message: 'Schedule created successfully',
      scheduledTime: responseTime,
      currentTime: formatTimeForResponse(now.getTime(), timezone),
      delayMinutes: Math.round((scheduleTime.getTime() - now.getTime()) / 60000),
      originalTimeInput: time,
      detectedTimezone: timezone ? `UTC${timezone >= 0 ? '+' : ''}${Math.floor(Math.abs(timezone) / 60)}:${(Math.abs(timezone) % 60).toString().padStart(2, '0')}` : 'auto/UTC'
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

// API: Get schedules for a device with timezone support
app.get('/schedules/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { timezone } = req.query; // Optional timezone for response formatting
    const clientTimezone = timezone ? parseInt(timezone) : undefined;
    
    const deviceSchedules = [];
    
    schedules.forEach((schedule) => {
      if (schedule.deviceId === deviceId) {
        deviceSchedules.push({
          ...schedule,
          scheduledTime: formatTimeForResponse(schedule.time, clientTimezone),
          originalTimezone: schedule.originalTimezone
        });
      }
    });

    res.status(200).json(deviceSchedules);
  } catch (err) {
    console.error('Error fetching schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// API: Get all schedules with timezone support
app.get('/schedules', async (req, res) => {
  try {
    const { timezone } = req.query; // Optional timezone for response formatting
    const clientTimezone = timezone ? parseInt(timezone) : undefined;
    
    const allSchedules = Array.from(schedules.values())
      .map(schedule => ({
        ...schedule,
        scheduledTime: formatTimeForResponse(schedule.time, clientTimezone),
        originalTimezone: schedule.originalTimezone
      }))
      .sort((a, b) => a.time - b.time);

    res.status(200).json(allSchedules);
  } catch (err) {
    console.error('Error fetching all schedules:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint with timezone-aware information
app.get('/health', (req, res) => {
  const { timezone } = req.query;
  const clientTimezone = timezone ? parseInt(timezone) : undefined;
  const now = getCurrentUTCTime();
  const schedulesArray = Array.from(schedules.values());
  const upcomingSchedules = schedulesArray.map(s => ({
    id: s.id,
    deviceName: s.deviceName,
    action: s.action,
    scheduledTime: formatTimeForResponse(s.time, clientTimezone),
    minutesUntilExecution: Math.round((s.time - now.getTime()) / 60000),
    originalTimezone: s.originalTimezone
  }));

  const status = {
    server: 'running',
    mqtt: mqttClient && mqttClient.connected ? 'connected' : 'disconnected',
    devices: devices.size,
    activeSchedules: schedules.size,
    timeouts: scheduleTimeouts.size,
    uptime: Math.round(process.uptime()),
    currentTime: formatTimeForResponse(now.getTime(), clientTimezone),
    serverTimezone: 'UTC',
    upcomingSchedules: upcomingSchedules.sort((a, b) => a.minutesUntilExecution - b.minutesUntilExecution)
  };
  res.status(200).json(status);
});

// Keep-alive endpoint for Render
app.get('/ping', (req, res) => {
  res.status(200).json({ message: 'pong', timestamp: new Date().toISOString() });
});

// API: Get server timezone info
app.get('/timezone', (req, res) => {
  const now = new Date();
  res.status(200).json({
    serverTime: {
      utc: now.toISOString(),
      timestamp: now.getTime()
    },
    info: {
      message: 'Server operates in UTC. Send timezone offset in minutes for local time conversion.',
      examples: {
        'UTC+0': 0,
        'UTC+1 (CET)': 60,
        'UTC-5 (EST)': -300,
        'UTC+5:30 (IST)': 330
      }
    }
  });
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
      console.log(`Timezone info available at http://localhost:${PORT}/timezone`);
      console.log(`Server operates in UTC timezone`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();