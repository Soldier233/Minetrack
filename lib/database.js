const mysql = require('mysql')

const logger = require('./logger')

const config = require('../config')
const { TimeTracker } = require('./time')

class Database {
  constructor (app) {
    this._app = app

    // eslint-disable-next-line new-cap
    this._sql = new mysql.createConnection(
      config.mysql
    )
    logger.log('info', '加载mysql完成')
  }

  loadGraphPoints (graphDuration, callback) {
    // Query recent pings
    const endTime = TimeTracker.getEpochMillis()
    const startTime = endTime - graphDuration

    this.getRecentPings(startTime, endTime, pingData => {
      const relativeGraphData = []

      for (const row of pingData) {
        // Load into temporary array
        // This will be culled prior to being pushed to the serverRegistration
        let graphData = relativeGraphData[row.server]
        if (!graphData) {
          relativeGraphData[row.server] = graphData = [[], []]
        }

        // DANGER!
        // This will pull the timestamp from each row into memory
        // This is built under the assumption that each round of pings shares the same timestamp
        // This enables all timestamp arrays to have consistent point selection and graph correctly
        graphData[0].push(row.time)
        this._app.timeTracker.newPointTimestamp(row.time)
        graphData[1].push(row.online)
      }

      Object.keys(relativeGraphData).forEach(ip => {
        // Match IPs to serverRegistration object
        for (const serverRegistration of this._app.serverRegistrations) {
          if (serverRegistration.data.name === ip) {
            const graphData = relativeGraphData[ip]
            // Push the data into the instance and cull if needed
            serverRegistration.loadGraphPoints(startTime, graphData[0], graphData[1])

            break
          }
        }
      })

      // Since all timestamps are shared, use the array from the first ServerRegistration
      // This is very dangerous and can break if data is out of sync
      if (Object.keys(relativeGraphData).length > 0) {
        const serverIp = Object.keys(relativeGraphData)[0]
        const timestamps = relativeGraphData[serverIp][0]

        this._app.timeTracker.loadGraphPoints(startTime, timestamps)
      }

      callback()
    })
  }

  loadRecords (callback) {
    let completedTasks = 0

    this._app.serverRegistrations.forEach(serverRegistration => {
      // Find graphPeaks
      // This pre-computes the values prior to clients connecting
      serverRegistration.findNewGraphPeak()

      // Query recordData
      // When complete increment completeTasks to know when complete
      this.getRecord(serverRegistration.data.ip, (hasRecord, playerCount, timestamp) => {
        if (hasRecord) {
          serverRegistration.recordData = {
            playerCount,
            timestamp: TimeTracker.toSeconds(timestamp)
          }
        }

        // Check if completedTasks hit the finish value
        // Fire callback since #readyDatabase is complete
        if (++completedTasks === this._app.serverRegistrations.length) {
          callback()
        }
      })
    })
  }

  getRecentPings (startTime, endTime, callback) {
    this._sql.query('SELECT `time`,`server`,`online` FROM netease_online WHERE `time` >= ? AND `time` <= ?', [
      startTime,
      endTime
    ], (err, data) => {
      if (err) {
        logger.log('error', 'Cannot get recent pings')
        throw err
      }
      callback(data)
    })
  }

  getRecord (ip, callback) {
    this._sql.query('SELECT MAX(`online`), `time` FROM netease_online WHERE server = ?', [
      ip
    ], (err, data) => {
      if (err) {
        logger.log('error', `Cannot get ping record for ${ip}`)
        throw err
      }

      // For empty results, data will be length 1 with [null, null]
      const playerCount = data[0]['MAX(`online`)']
      const timestamp = data[0].time

      // Allow null timestamps, the frontend will safely handle them
      // This allows insertion of free standing records without a known timestamp
      if (playerCount !== null) {
        // eslint-disable-next-line node/no-callback-literal
        callback(true, playerCount, timestamp)
      } else {
        // eslint-disable-next-line node/no-callback-literal
        callback(false)
      }
    })
  }
}

module.exports = Database
