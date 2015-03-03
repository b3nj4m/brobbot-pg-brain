{Brain, User} = require 'brobbot'
Url = require "url"
pg = require "pg"
Q = require "q"
_ = require "lodash"
msgpack = require "msgpack"

class PgBrain extends Brain
  constructor: (@robot, @useMsgpack = true) ->
    super(@robot)

    #TODO heroku postgres?
    pgUrl = if process.env.PGTOGO_URL?
                 pgUrlEnv = "PGTOGO_URL"
                 process.env.PGTOGO_URL
               else if process.env.PGCLOUD_URL?
                 pgUrlEnv = "PGCLOUD_URL"
                 process.env.PGCLOUD_URL
               else if process.env.BOXEN_PG_URL?
                 pgUrlEnv = "BOXEN_PG_URL"
                 process.env.BOXEN_PG_URL
               else if process.env.PG_URL?
                 pgUrlEnv = "PG_URL"
                 process.env.PG_URL
               else
                 'postgres://root:password@localhost/brobbot'

    if pgUrlEnv?
      @robot.logger.info "Discovered pg from #{pgUrlEnv} environment variable"
    else
      @robot.logger.info "Using default pg on localhost"

    @info = Url.parse pgUrl, true
    @prefix = process.env.BROBBOT_PG_DATA_PREFIX or 'data:'
    @prefixRegex = new RegExp("^#{@prefix}")
    @dbName = process.env.BROBBOT_PG_DB_NAME or 'brobbot'
    @tableName = @info.path?.replace('/', '') or 'brobbot'

    @client = new pg.Client(pgUrl)
    @connected = Q.ninvoke @client, 'connect'

    @connected.then =>
      @robot.logger.info "Successfully connected to pg"
    @connected.fail (err) =>
      @robot.logger.error "Failed to connect to pg: " + err

    @ready = @connected.then => @initTable

  initTable: ->
    #TODO confirm varchar limits
    #TODO transactions?
    query = "CREATE DATABASE IF NOT EXISTS #{@dbName}; USE #{@dbName}; CREATE TABLE IF NOT EXISTS #{@tableName} (key varchar(255) NOT NULL, subkey varchar(255), value jsonb, UNIQUE KEY (key, subkey))"
    Q.ninvoke @client, 'query', query

  @query: (query, params) ->
    @ready.then =>
      Q.ninvoke(@client, 'query', query, params)

  updateValue: (key, value) ->
    @exists(key).then (exists) =>
      if exists
        return @query("UPDATE #{@tableName} SET value = $1 WHERE key = $2", [value, key])
      else
        return @query("INSERT INTO #{@tableName} (key, value) VALUES ($1, $2)", [key, value])

  updateSubValue: (key, subkey, value) ->
    @subExists(key, subkey).then (exists) =>
      if exists
        return @query("UPDATE #{@tableName} SET value = $1 WHERE key = $2 AND subkey = $3", [value, key, subkey])
      else
        return @query("INSERT INTO #{@tableName} (key, value, subkey) VALUES ($1, $2, $3)", [key, value, subkey])

  updateSet: (key, values) ->
    @updateValue(key, _.unique(values))

  getValues: (key, subkey) ->
    params = [key]
    subkeyPart = ""

    if subkey
      subkeyPart = "AND subkey = $3"
      params.push(subkey)

    @query("SELECT value FROM #{@tableName} WHERE key = $1 #{subkeyPart}", params).then (results) =>
      _.map results, (result) => @deserialize result

  llen: (key) ->
    @query("SELECT json_array_length(value) FROM #{@tableName} WHERE key = $1", [@key(key)]).then (results) -> results[0] or 0

  lset: (key, index, value) ->
    #TODO prolly doesn't work
    @query("UPDATE #{@tableName} SET value -> $1 = $2 WHERE key = $3", [index, @serialize(value), @key(key)])

  linsert: (key, placement, pivot, value) ->
    @lgetall(key).then (values) =>
      idx = values.indexOf(pivot)

      if idx is -1
        return -1

      if placement is 'AFTER'
        idx = idx + 1

      values.splice(idx, 0, value)

      @updateValue(@key(key), values)

  lpush: (key, value) ->
    @lgetall(key).then (values) =>
      values.unshift(value)
      @updateValue(@key(key), values)

  rpush: (key, value) ->
    @lgetall(key).then (values) =>
      values.push(value)
      @updateValue(@key(key), values)

  lpop: (key) ->
    @lgetall(key).then (values) =>
      value = values.shift()
      @updateValue(@key(key), values).then -> value

  rpop: (key) ->
    @lgetall(key).then (values) =>
      value = values.pop()
      @updateValue(@key(key), values).then -> value

  lindex: (key, index) ->
    @query("SELECT value->$1 FROM #{@tableName} WHERE key = $2", [index, @key(key)]).then (result) ->
      result[0] or -1

  lgetall: (key) ->
    @getValues(@key(key))

  lrange: (key, start, end) ->
    @lgetall(key).then (values) =>
      #TODO inclusiveness?
      values.slice(start, end)

  lrem: (key, value) ->
    @lgetall(key).then (values) =>
      idx = values.indexOf(value)
      count = 0

      while idx isnt -1
        count++
        values.splice(idx, 1)
        idx = values.indexOf(value)

      @updateValue(@key(key), values).then -> count

  sadd: (key, value) ->
    @lgetall(key).then (values) =>
      values.push(value)
      @updateSet(@key(key), values)

  sismember: (key, value) ->
    @query("SELECT 1 FROM #{@tableName} WHERE value @> '[]' AND key = $1 AND value ? $2", [@key(key), value]).then (results) -> results.length > 0

  srem: (key, value) ->
    @lrem(key, value)

  scard: (key) ->
    @llen(key)

  spop: (key) ->
    @rpop(key)

  srandmember: (key) ->
    @smembers(key).then (values) =>
      values[_.random(values.length - 1)]

  smembers: (key) ->
    @lgetall(key)

  keys: (searchKey = '') ->
    searchKey = @key searchKey
    @query("SELECT key FROM #{@tableName} WHERE key LIKE $1", ["#{searchKey}%"])

  type: (key) ->
    #TODO
    @query("")

  types: (keys) ->
    Q.all(_.map(keys, (key) => @type(key)))

  unkey: (key) ->
    key.replace(@prefixRegex, '')

  key: (key) ->
    "#{@prefix}#{key}"

  usersKey: () ->
    "users"

  subExists: (table, key) ->
    @query("SELECT 1 FROM #{@tableName} WHERE key = $1 AND subkey = $2 LIMIT 1", [@key(table), key]).then (results) -> results.length > 0

  exists: (key) ->
    @query("SELECT 1 FROM #{@tableName} WHERE key = $1 LIMIT 1", [@key(key)]).then (results) -> results.length > 0

  get: (key) ->
    @getValues(@key(key)).then (results) -> results[0]

  set: (key, value) ->
    @updateValue(@key(key), value)

  remove: (key) ->
    @query("DELETE FROM #{@tableName} WHERE key = $1", [@key(key)])

  # Public: increment the value by num atomically
  #
  # Returns promise
  incrby: (key, num) ->
    updateValue = @exists(key).then (exists) =>
      if exists
        return @query("UPDATE #{@tableName} SET value = value + $1 WHERE key = $2", [num, @key(key)])
      else
        return @query("INSERT INTO #{@tableName} (key, value) VALUES ($1, $2)", [@key(key), num])

    updateValue.then => @get(key)

  # Public: Get all the keys for the given hash table name
  #
  # Returns promise for array.
  hkeys: (table) ->
    @query("SELECT subkey FROM #{@tableName} WHERE key = $1", [table])

  # Public: Get all the values for the given hash table name
  #
  # Returns promise for array.
  hvals: (table) ->
    @getValues(@key(table))

  # Public: get the size of the hash table
  #
  # Returns promise for int.
  hlen: (table) ->
    @query("SELECT COUNT(*) FROM #{@tableName} WHERE key = $1 GROUP BY key", [@key(table)]).then (results) -> result[0] or 0

  # Public: Set a value in the specified hash table
  #
  # Returns promise for the value.
  hset: (table, key, value) ->
    @updateValue(@key(table), value, key)

  # Public: Get a value from the specified hash table.
  #
  # Returns: promise for the value.
  hget: (table, key) ->
    @getValues(@key(table), key).then (results) -> results[0]

  # Public: Delete a field from the specified hash table.
  #
  # Returns promise
  hdel: (table, key) ->
    @query("DELETE FROM #{@tableName} WHERE key = $1 AND subkey = $2", [@key(table), key])

  # Public: Get the whole hash table as an object.
  #
  # Returns: promise for object.
  hgetall: (table) ->
    @query("SELECT subkey, value FROM #{@tableName} WHERE key = $1", [@key(table)]).then (results) =>
      _.zip(_.map(results, (result) => [result.subkey, result.value]))

  # Public: increment the hash value by num atomically
  #
  # Returns promise
  hincrby: (table, key, num) ->
    table = @key(table)

    updateValue = @subExists(table, key).then (exists) =>
      if exists
        return @query("UPDATE #{@tableName} SET value = value + $1 WHERE key = $2 AND subkey = $3", [num, table, key])
      else
        return @query("INSERT INTO #{@tableName} (key, subkey, value) VALUES ($1, $2)", [table, key, num])

  close: ->
    @client.end()

  # Public: Perform any necessary pre-set serialization on a value
  #
  # Returns serialized value
  serialize: (value) ->
    JSON.stringify(value)

  # Public: Perform any necessary post-get deserialization on a value
  #
  # Returns deserialized value
  deserialize: (value) ->
    JSON.parse(value.toString())

  # Public: Perform any necessary pre-set serialization on a user
  #
  # Returns serialized user
  serializeUser: (user) ->
    @serialize user

  # Public: Perform any necessary post-get deserializtion on a user
  #
  # Returns a User
  deserializeUser: (obj) ->
    if obj
      obj = @deserialize obj

      if obj and obj.id
        return new User obj.id, obj

    null

  # Public: Get an Array of User objects stored in the brain.
  #
  # Returns promise for an Array of User objects.
  users: ->
    @getValues(@usersKey()).then (results) =>
      _.map(results, (result) => @deserializeUser(result))

  # Public: Add a user to the data-store
  #
  # Returns promise for user
  addUser: (user) ->
    @updateSubValue(@usersKey(), user.id, user)

  # Public: Get or create a User object given a unique identifier.
  #
  # Returns promise for a User instance of the specified user.
  userForId: (id, options) ->
    @getValues(@usersKey(), id).then (results) =>
      user = results[0]
      if user
        user = @deserializeUser user

      if !user or (options and options.room and (user.room isnt options.room))
        return @addUser(new User(id, options))

      return user

  # Public: Get a User object given a name.
  #
  # Returns promise for a User instance for the user with the specified name.
  userForName: (name) ->
    name = name.toLowerCase()

    @query("SELECT value FROM #{@tableName} WHERE key = $1 AND value ->> 'name' = $2", [@usersKey(), name]).then (results) => @deserializeUser(results[0])

  # Public: Get all users whose names match fuzzyName. Currently, match
  # means 'starts with', but this could be extended to match initials,
  # nicknames, etc.
  #
  # Returns promise an Array of User instances matching the fuzzy name.
  usersForRawFuzzyName: (fuzzyName) ->
    fuzzyName = fuzzyName.toLowerCase()

    @query("SELECT value FROM #{@tableName} WHERE key = $1 AND value ->> 'name' LIKE $2", [@usersKey(), "#{fuzzyName}%").then (results) => @deserializeUser(results[0])

  # Public: If fuzzyName is an exact match for a user, returns an array with
  # just that user. Otherwise, returns an array of all users for which
  # fuzzyName is a raw fuzzy match (see usersForRawFuzzyName).
  #
  # Returns promise an Array of User instances matching the fuzzy name.
  usersForFuzzyName: (fuzzyName) ->
    fuzzyName = fuzzyName.toLowerCase()

    @usersForRawFuzzyName(fuzzyName).then (matchedUsers) ->
      exactMatch = _.find matchedUsers, (user) ->
        user.name.toLowerCase() is fuzzyName

      exactMatch and [exactMatch] or matchedUsers

module.exports = PgBrain
