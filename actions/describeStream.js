
module.exports = function describeStream(store, data, cb) {

  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    delete stream._seqIx
    delete stream._tags

    //console.log("describeStream: " + JSON.stringify(stream.Shards))
    var now = Date.now()

    var filteredStream = JSON.parse(JSON.stringify(stream))
    var newShards = filteredStream.Shards.filter(shard => shard.ExpirationTime === null || shard.ExpirationTime === undefined || shard.ExpirationTime > now)
    filteredStream.Shards = newShards

    //console.log("describeStream filtered: " + JSON.stringify(filteredStream.Shards))

    cb(null, {StreamDescription: filteredStream})
  })
}



