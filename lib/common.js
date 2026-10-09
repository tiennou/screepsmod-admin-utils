const assert = require('assert')
const EventEmitter = require('events').EventEmitter
const utils = require('@screeps/backend/lib/utils.js')
const YAML = require('yamljs')
const fs = require('fs')
const util = require('util')
const readFile = util.promisify(fs.readFile)

function isEqual (a, b) {
  try {
    assert.deepStrictEqual(a, b)
    return true
  } catch {
    return false
  }
}

module.exports = (config) => {
  const { storage: { db, env, pubsub }, constants: C } = config.common
  Object.assign(env.keys, {
    SHARD_NAME: 'shardName',
    TICK_TIMING: 'tickTiming',
    SOCKET_UPDATE_RATE: 'socketUpdateRate',
    LAST_TICKS: 'lastTicks',
    MAX_LOOP_DURATION: 'maxLoopDuration'
  })

  config.utils = new EventEmitter()
  Object.assign(config.utils, {
    tickTiming: [],
    test () {
      console.log('Testing!')
    },
    errCatch (fn) {
      return (req, res) => {
        fn(req, res).catch(err => {
          console.error(req.url, err)
          res.status(500).send({ error: err.stack })
        })
      }
    },
    async addNPCTerminals (interval = 10) {
      interval = Math.max(interval, 1)
      let count = 0
      const rooms = await db.rooms.find({ bus: true })
      const roomNames = rooms.map(r => r._id)
      const ps = roomNames.map(async room => {
        let [x, y] = utils.roomNameToXY(room)
        if (x < 0) x = 1 + x
        if (y < 0) y = 1 + y
        if (x % interval === 0 && y % interval === 0) {
          const res = await db['rooms.objects'].findOne({ type: 'terminal', room })
          if (res) return
          count++
          await db['rooms.objects'].insert({
            type: 'terminal',
            room,
            x: 0,
            y: 0,
            npc: true,
            store: {},
            storeCapacity: C.TERMINAL_CAPACITY
          })
        }
      })
      await Promise.all(ps)
      return `Added ${count} terminals`
    },
    removeNPCTerminals () {
      return db['rooms.objects'].removeWhere({ type: 'terminal', npc: true })
    },
    async removeBots () {
      if (!config.auth || typeof config.auth.removeUser !== 'function') {
        return 'screepsmod-auth removeUser() is not available'
      }
      const users = await db.users.find()
      const ids = users.filter(u => u.bot).map(u => u._id)
      for (const id of ids) {
        await config.auth.removeUser(id)
      }
      return 'Bots removed successfully'
    },
    setSocketUpdateRate (value) {
      if (!value) return 'Value required'
      config.common.storage.pubsub.publish('setSocketUpdateRate', value)
      return 'Socket update rate set to ' + value + 'ms'
    },
    getSocketUpdateRate () {
      return env.get(env.keys.SOCKET_UPDATE_RATE).then(value => `Socket update rate is ${value}ms`)
    },
    setMaxLoopDuration (value) {
      const ms = parseInt(value, 10)
      if (!(ms > 0)) return 'Positive value required'
      env.set(env.keys.MAX_LOOP_DURATION, ms)
      pubsub.publish('setMaxLoopDuration', ms)
      return 'Max loop duration set to ' + ms + 'ms'
    },
    getMaxLoopDuration () {
      return env.get(env.keys.MAX_LOOP_DURATION).then(value => `Max loop duration is ${value || 5000}ms`)
    },
    async setShardName (value) {
      await env.set(env.keys.SHARD_NAME, value)
      pubsub.publish('setShardName', value)
    },
    async reloadConfig () {
      let filename
      const configFiles = ['config.yml', 'config.yaml']
      for (const file of configFiles) {
        try {
          fs.statSync(file)
          filename = file
        } catch (_) { }
      }
      if (!filename) return
      console.log(`Loading config from ${filename}`)
      try {
        const { serverConfig = {} } = YAML.parse(await readFile(filename, 'utf8'))
        const conf = serverConfig
        console.log('Applying config', conf)
        const { common: { storage: { env, pubsub } } } = config
        const { constants, shardName, socketUpdateRate, tickRate, maxLoopDuration, welcomeText, whitelist, statsToken } = conf
        if (config.backend) {
          if (welcomeText) {
            config.backend.welcomeText = welcomeText
          }
        }
        if (config.common) {
          if (constants) {
            const consts = Object.assign({}, config.common.constants, constants)
            pubsub.publish('setConstants', JSON.stringify(consts))
          }
        }
        if (tickRate) {
          env.set(env.keys.MAIN_LOOP_MIN_DURATION, tickRate)
          pubsub.publish('setTickRate', tickRate)
        }
        const maxLoopMs = parseInt(maxLoopDuration, 10)
        if (maxLoopMs > 0) {
          env.set(env.keys.MAX_LOOP_DURATION, maxLoopMs)
          pubsub.publish('setMaxLoopDuration', maxLoopMs)
        }
        if (socketUpdateRate) {
          pubsub.publish('setSocketUpdateRate', socketUpdateRate)
        }
        if (whitelist) {
          env.set(env.keys.WHITELIST, JSON.stringify(Array.from(whitelist)))
        }
        if (shardName) {
          await env.set(env.keys.SHARD_NAME, shardName)
          pubsub.publish('setShardName', shardName)
        }
        if (statsToken) {
          config.utils.statsToken = statsToken
        }
        const lastConfig = config.utils.config || {}
        config.utils.config = conf
        config.utils.emit('config', conf)
        for (const [k, v] of Object.entries(conf)) {
          if (!isEqual(lastConfig[k], v)) {
            config.utils.emit(`config:update:${k}`, v)
          }
        }
      } catch (err) {
        console.error('Error reloading config:', err)
      }
    },
    banUser (username, remove = false) {
      if (!config.auth || typeof config.auth.banUser !== 'function') {
        return 'screepsmod-auth banUser() is not available'
      }
      return config.auth.banUser(username, remove)
    },
    unbanUser (username) {
      if (!config.auth || typeof config.auth.unbanUser !== 'function') {
        return 'screepsmod-auth unbanUser() is not available'
      }
      return config.auth.unbanUser(username)
    },
    async respawnUser (username) {
      const user = await db.users.findOne({ usernameLower: username.toLowerCase() })
      if (!user) {
        return `Can't find user "${username}"`
      }
      await utils.respawnUser(user._id)
      console.log(`Respawned user "${user.username}" ${user._id}`)
      return `Respawned user "${user.username}" ${user._id}`
    }
  })

  config.utils.addNPCTerminals._help = 'addNPCTerminals(interval = 10) Add NPC terminals every interval room'
  config.utils.removeNPCTerminals._help = 'removeNPCTerminals() Remove all NPC terminals'
  config.utils.removeBots._help = 'removeBots() Remove all bot users'
  config.utils.setSocketUpdateRate._help = 'setSocketUpdateRate(value) Sets socket update rate (in ms)'
  config.utils.getSocketUpdateRate._help = 'getSocketUpdateRate() Returns current socket update rate'
  config.utils.setMaxLoopDuration._help = 'setMaxLoopDuration(value) Sets main loop stall reset (in ms). 5000ms minimum'
  config.utils.getMaxLoopDuration._help = 'getMaxLoopDuration() Returns current max loop duration'
  config.utils.setShardName._help = 'setShardName(value) Sets the shard name'
  config.utils.reloadConfig._help = 'reloadConfig() Reloads the serverConfig section of config.yml'
  config.utils.banUser._help = (config.auth && config.auth.banUser && config.auth.banUser._help) ||
    'banUser(username, remove = false) Ban the specified user from the server.\n' +
    '\tPassing `false` will suspend their CPU usage, `true` will delete their data entirely.'
  config.utils.unbanUser._help = (config.auth && config.auth.unbanUser && config.auth.unbanUser._help) ||
    'unbanUser(username) Unban the specified user from the server.'
  config.utils.respawnUser._help = 'respawnUser(username) Respawns the specified user.'

  Object.defineProperty(config.utils, '_help', {
    get () { // Using a getter here so that loaded services are also included
      const funcs = []
      for (const k in config.utils) {
        const help = config.utils[k] && config.utils[k]._help
        if (help) funcs.push(help)
      }
      return `Admin Utilities\n${funcs.map(f => `* ${f}`).join('\n')}`
    }
  })
}
