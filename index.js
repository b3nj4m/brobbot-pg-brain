var brobbot = require('brobbot');
var Brain = brobbot.Brain;
var User = brobbot.User;
var Url = require("url");
var pg = require("pg").native;
var Q = require("q");
var _ = require("lodash");

function PgBrain(robot) {
  Brain.apply(this, arguments);

  var self = this;
  var pgUrl = null;
  var pgUrlEnv = null;

  this.robot = robot;
  this.currentTransaction = Q();

  _.each(process.env, function(envVal, envVar) {
    if (/^HEROKU_POSTGRESQL_[A-Z0-9]+_URL$/.test(envVar)) {
      pgUrlEnv = envVar;
      pgUrl = envVal;
      return false;
    }
  });

  if (!pgUrl) {
    var envVars = ['POSTGRESQL_URL', 'DATABASE_URL'];

    for (var i = 0; i < envVars.length; i++) {
      if (process.env[envVars[i]]) {
        pgUrlEnv = envVars[i];
        pgUrl = process.env[envVars[i]];
        break;
      }
    }
  }

  pgUrl = pgUrl || 'postgres://user:password@localhost/brobbot';

  if (pgUrlEnv) {
    this.robot.logger.info("Discovered pg from " + pgUrlEnv + " environment variable");
  }
  else {
    this.robot.logger.info("Using default pg on localhost");
  }

  this.info = Url.parse(pgUrl, true);

  this.prefix = process.env.BROBBOT_PG_DATA_PREFIX || 'data';
  this.prefixRegex = new RegExp("^" + this.prefix + ":");

  this.tableName = process.env.BROBBOT_PG_TABLE_NAME || 'brobbot';

  this.client = new pg.Client(pgUrl);

  this.connected = Q.ninvoke(this.client, 'connect');

  this.connected.then(function() {
    return self.robot.logger.info("Successfully connected to pg");
  });

  this.connected.fail(function(err) {
    return self.robot.logger.error("Failed to connect to pg: " + err);
  });

  this.ready = this.connected.then(this.checkVersion.bind(this)).then(this.initTable.bind(this));
}

PgBrain.prototype = Object.create(Brain.prototype);
PgBrain.prototype.constructor = PgBrain;

PgBrain.prototype.checkVersion = function() {
  var self = this;

  var query = "SELECT VERSION()";

  return Q.ninvoke(this.client, 'query', query).then(function(results) {
    if (results.rows.length === 0 || parseFloat(results.rows[0].version.replace(/^postgresql /i)) < 9.4) {
      return self.robbot.logger.error("Postgres version must be at least 9.4");
    }
  });
};

PgBrain.prototype.initTable = function() {
  var query = "CREATE TABLE IF NOT EXISTS " + this.tableName + " (key varchar(255) NOT NULL, subkey varchar(255), isset boolean not null default false, value jsonb, UNIQUE (key, subkey))";
  return Q.ninvoke(this.client, 'query', query);
};

PgBrain.prototype.transaction = function(fn) {
  return this.currentTransaction = this.currentTransaction.then(this.runTransaction.bind(this, fn));
};

PgBrain.prototype.runTransaction = function(fn) {
  var self = this;

  return this.query("BEGIN").then(fn).then(function(result) {
    return self.query("COMMIT").then(function() {
      return result;
    });
  }).fail(function(err) {
    return self.query("COMMIT").then(function() {
      throw err;
    });
  });
};

PgBrain.prototype.query = function(query, params) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'query', query, params).then(function(results) {
      return results.rows;
    }).fail(function(err) {
      self.robot.logger.error('PGSQL error:', err.stack);
      return null;
    });
  });
};

PgBrain.prototype.updateValue = function(key, value, isSet) {
  var self = this;

  if (isSet === undefined) {
    isSet = false;
  }

  return this.keyExists(key).then(function(exists) {
    value = self.serialize(value);
    if (exists) {
      return self.query("UPDATE " + self.tableName + " SET value = $1 WHERE key = $2", [value, key]);
    }
    else {
      return self.query("INSERT INTO " + self.tableName + " (key, value, isset) VALUES ($1, $2, $3)", [key, value, isSet]);
    }
  });
};

PgBrain.prototype.updateSubValue = function(key, subkey, value) {
  var self = this;

  value = this.serialize(value);

  return this.subkeyExists(key, subkey).then(function(exists) {
    if (exists) {
      return self.query("UPDATE " + self.tableName + " SET value = $1 WHERE key = $2 AND subkey = $3", [value, key, subkey]);
    }
    else {
      return self.query("INSERT INTO " + self.tableName + " (key, value, subkey) VALUES ($1, $2, $3)", [key, value, subkey]);
    }
  });
};

PgBrain.prototype.getValues = function(key, subkey) {
  var self = this;

  var params = [key];
  var subkeyPart = "";

  if (subkey !== undefined) {
    subkeyPart = "AND subkey = $2";
    params.push(subkey);
  }

  return this.query("SELECT value FROM " + this.tableName + " WHERE key = $1 " + subkeyPart, params).then(function(results) {
    return _.map(results, function(result) {
      return self.deserialize(result.value);
    });
  });
};

PgBrain.prototype.reset = function() {
  return this.query("DELETE FROM " + this.tableName).then(function() {
    return Q();
  });
};

PgBrain.prototype.llen = function(key) {
  return this.query("SELECT jsonb_array_length(value) AS length FROM " + this.tableName + " WHERE key = $1 AND value @> '[]'", [this.key(key)]).then(function(results) {
    return results.length > 0 ? results[0].length : null;
  });
};

PgBrain.prototype.lset = function(key, index, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values[index] = value;
      return self.updateValue(self.key(key), values);
    });
  });
};

PgBrain.prototype.lfindindex = function(values, value) {
  var self = this;

  var serializedValue = this.serialize(value);

  return _.findIndex(values, function(value) {
    return self.serialize(value) === serializedValue;
  });
};

PgBrain.prototype.linsert = function(key, placement, pivot, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      var idx = self.lfindindex(values, pivot);

      if (idx === -1) {
        return -1;
      }

      if (placement === 'AFTER') {
        idx = idx + 1;
      }

      values.splice(idx, 0, value);

      return self.updateValue(self.key(key), values);
    });
  });
};

PgBrain.prototype.lpush = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values.unshift(value);
      return self.updateValue(self.key(key), values);
    });
  });
};

PgBrain.prototype.rpush = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values.push(value);
      return self.updateValue(self.key(key), values);
    });
  });
};

PgBrain.prototype.lpop = function(key) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      var value;
      if (values) {
        value = values.shift();
        return self.updateValue(self.key(key), values).then(function() {
          return value;
        });
      }
      else {
        return null;
      }
    });
  });
};

PgBrain.prototype.rpop = function(key) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      if (values) {
        var value = values.pop();

        return self.updateValue(self.key(key), values).then(function() {
          return value;
        });
      }
      else {
        return null;
      }
    });
  });
};

PgBrain.prototype.lindex = function(key, index) {
  var self = this;

  return this.query("SELECT value -> $1::int AS value FROM " + this.tableName + " WHERE key = $2 AND value @> '[]'", [index, this.key(key)]).then(function(results) {
    return self.deserialize(results.length > 0 ? results[0] : null);
  });
};

PgBrain.prototype.lgetall = function(key) {
  return this.getValues(this.key(key)).then(function(results) {
    return results && results.length > 0 ? results[0] : null;
  });
};

PgBrain.prototype.lrange = function(key, start, end) {
  return this.lgetall(key).then(function(values) {
    return values ? values.slice(start, end + 1) : null;
  });
};

PgBrain.prototype.lrem = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      var serialized = self.serialize(value);
      var newValues = _.without(_.map(values, self.serialize.bind(self)), serialized);

      //TODO inefficient
      newValues = _.map(newValues, function(value) {
        return self.deserialize(value, true);
      });

      return self.updateValue(self.key(key), newValues).then(function() {
        return values.length - newValues.length;
      });
    });
  });
};

PgBrain.prototype.sadd = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.sismember(key, value).then(function(isMemeber) {
      if (isMemeber) {
        return -1;
      }
      return self.lgetall(key).then(function(values) {
        values = values || [];
        values.push(value);
        return self.updateValue(self.key(key), values, true);
      });
    });
  });
};

PgBrain.prototype.sismember = function(key, value) {
  return this.query("SELECT 1 FROM (SELECT jsonb_array_elements(value) AS elem FROM " + this.tableName + " WHERE isset = true AND value @> '[]' AND key = $1) AS foo WHERE foo.elem = $2::jsonb", [this.key(key), this.serialize(value)]).then(function(results) {
    return results.length > 0;
  });
};

PgBrain.prototype.srem = function(key, value) {
  return this.lrem(key, value);
};

PgBrain.prototype.scard = function(key) {
  return this.llen(key);
};

PgBrain.prototype.spop = function(key) {
  return this.rpop(key);
};

PgBrain.prototype.srandmember = function(key) {
  var self = this;

  return this.query("SELECT foo.elem FROM (SELECT jsonb_array_elements(value) AS elem FROM " + this.tableName + " WHERE value @> '[]' AND key = $1) AS foo OFFSET (random() * (SELECT (CASE WHEN jsonb_array_length(value) > 0 THEN jsonb_array_length(value) - 1 ELSE 0 END) FROM " + this.tableName + " WHERE value @> '[]' AND key = $1)) LIMIT 1", [this.key(key)]).then(function(results) {
    if (results.length === 0) {
      return null;
    }
    return self.deserialize(results[0].elem);
  });
};

PgBrain.prototype.smembers = function(key) {
  return this.lgetall(key);
};

PgBrain.prototype.keys = function(searchKey) {
  var self = this;

  if (searchKey === undefined) {
    searchKey = '';
  }

  searchKey = this.key(searchKey);

  return this.query("SELECT DISTINCT key FROM " + this.tableName + " WHERE key ILIKE $1", [searchKey + "%"]).then(function(results) {
    return _.map(results, function(result) {
      return self.unkey(result.key);
    });
  });
};

PgBrain.prototype.type = function(key) {
  return this.query("SELECT (CASE WHEN isset THEN 'set' WHEN value @> '[]' THEN 'list' WHEN subkey IS NOT NULL THEN 'hash' ELSE 'object' END) AS type FROM " + this.tableName + " WHERE key = $1 LIMIT 1", [this.key(key)]).then(function(results) {
    return results.length > 0 ? results[0].type : null;
  });
};

PgBrain.prototype.types = function(keys) {
  return Q.all(_.map(keys, function(key) {
    return self.type(key);
  }));
};

PgBrain.prototype.unkey = function(key) {
  return key.replace(this.prefixRegex, '');
};

PgBrain.prototype.key = function(key) {
  return this.prefix + ":" + key;
};

PgBrain.prototype.usersKey = function() {
  return "users";
};

PgBrain.prototype.subkeyExists = function(table, key) {
  return this.query("SELECT 1 FROM " + this.tableName + " WHERE key = $1 AND subkey = $2 LIMIT 1", [table, key]).then(function(results) {
    return results.length > 0;
  });
};

PgBrain.prototype.keyExists = function(key) {
  return this.query("SELECT 1 FROM " + this.tableName + " WHERE key = $1 LIMIT 1", [key]).then(function(results) {
    return results.length > 0;
  });
};

PgBrain.prototype.exists = function(key) {
  return this.keyExists(this.key(key));
};

PgBrain.prototype.get = function(key) {
  return this.getValues(this.key(key)).then(function(results) {
    return results.length > 0 ? results[0] : null;
  });
};

PgBrain.prototype.set = function(key, value) {
  return this.updateValue(this.key(key), value);
};

PgBrain.prototype.remove = function(key) {
  return this.query("DELETE FROM " + this.tableName + " WHERE key = $1", [this.key(key)]);
};

PgBrain.prototype.incrby = function(key, num) {
  var self = this;

  return this.transaction(function() {
    var updateValue = self.get(key).then(function(val) {
      key = self.key(key);

      if (val !== null) {
        num = val + num;
        return self.query("UPDATE " + self.tableName + " SET value = $1 WHERE key = $2", [num, key]);
      }
      else {
        return self.query("INSERT INTO " + self.tableName + " (key, value) VALUES ($1, $2)", [key, num]);
      }
    });

    return updateValue.then(function() {
      return num;
    });
  });
};

PgBrain.prototype.hkeys = function(table) {
  return this.query("SELECT subkey FROM " + this.tableName + " WHERE key = $1", [this.key(table)]).then(function(results) {
    return _.map(results, function(result) {
      return result.subkey;
    });
  });
};

PgBrain.prototype.hvals = function(table) {
  return this.getValues(this.key(table));
};

PgBrain.prototype.hlen = function(table) {
  return this.query("SELECT COUNT(*) AS count FROM " + this.tableName + " WHERE key = $1 GROUP BY key", [this.key(table)]).then(function(results) {
    return results.length > 0 ? parseInt(results[0].count) : null;
  });
};

PgBrain.prototype.hset = function(table, key, value) {
  return this.updateSubValue(this.key(table), key, value);
};

PgBrain.prototype.hget = function(table, key) {
  return this.getValues(this.key(table), key).then(function(results) {
    return results.length > 0 ? results[0] : null;
  });
};

PgBrain.prototype.hdel = function(table, key) {
  return this.query("DELETE FROM " + this.tableName + " WHERE key = $1 AND subkey = $2", [this.key(table), key]);
};

PgBrain.prototype.hgetall = function(table) {
  var self = this;

  return this.query("SELECT subkey, value FROM " + this.tableName + " WHERE key = $1", [this.key(table)]).then(function(results) {
    var map = new Map();

    _.each(results, function(result) {
      return map.set(result.subkey, self.deserialize(result.value));
    });

    return map;
  });
};

PgBrain.prototype.hincrby = function(table, key, num) {
  var self = this;

  return this.transaction(function() {
    var updateValue = self.hget(table, key).then(function(val) {
      table = self.key(table);

      if (val !== null) {
        num = val + num;
        return self.query("UPDATE " + self.tableName + " SET value = $1 WHERE key = $2 AND subkey = $3", [num, table, key]);
      }
      else {
        return self.query("INSERT INTO " + self.tableName + " (key, subkey, value) VALUES ($1, $2, $3)", [table, key, num]);
      }
    });

    return updateValue.then(function() {
      return num;
    });
  });
};

PgBrain.prototype.close = function() {
  return this.client.end();
};

PgBrain.prototype.serialize = function(value) {
  return JSON.stringify(value);
};

PgBrain.prototype.deserialize = function(value, force) {
  if (force) {
    return JSON.parse(value.toString());
  }

  /*
  #json apparently gets deserialized by pg
   */
  return value;
};

PgBrain.prototype.serializeUser = function(user) {
  return this.serialize(user);
};

PgBrain.prototype.deserializeUser = function(obj) {
  if (obj) {
    obj = this.deserialize(obj);
    if (obj && obj.id) {
      return new User(obj.id, obj);
    }
  }
  return null;
};

PgBrain.prototype.users = function() {
  var self = this;

  return this.getValues(this.usersKey()).then(function(results) {
    return _.map(results, function(result) {
      return self.deserializeUser(result.value);
    });
  });
};

PgBrain.prototype.addUser = function(user) {
  return this.updateSubValue(this.usersKey(), user.id, user);
};

PgBrain.prototype.userForId = function(id, options) {
  var self = this;

  return this.getValues(this.usersKey(), id).then(function(results) {
    var user = results[0];

    if (user) {
      user = self.deserializeUser(user);
    }

    if (!user || (options && options.room && (user.room !== options.room))) {
      return self.addUser(new User(id, options));
    }

    return user;
  });
};

PgBrain.prototype.userForName = function(name) {
  var self = this;

  name = name && name.toLowerCase() || '';

  return this.query("SELECT value FROM " + this.tableName + " WHERE key = $1 AND value ->> 'name' = $2", [this.usersKey(), name]).then(function(results) {
    return self.deserializeUser(results.length ? results[0].value : null);
  });
};

PgBrain.prototype.usersForRawFuzzyName = function(fuzzyName) {
  var self = this;

  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.query("SELECT value FROM " + this.tableName + " WHERE key = $1 AND value ->> 'name' ILIKE $2", [this.usersKey(), fuzzyName + "%"]).then(function(results) {
    return _.map(results, function(result) {
      return self.deserializeUser(result.value);
    });
  });
};

PgBrain.prototype.usersForFuzzyName = function(fuzzyName) {
  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.usersForRawFuzzyName(fuzzyName).then(function(matchedUsers) {
    var exactMatch = _.find(matchedUsers, function(user) {
      return user.name.toLowerCase() === fuzzyName;
    });
    return exactMatch && [exactMatch] || matchedUsers;
  });
};

module.exports = PgBrain;
