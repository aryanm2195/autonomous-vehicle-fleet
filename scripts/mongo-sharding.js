// Production sharding (MongoDB Atlas / sharded cluster) - project plan §10.1.
// Hashed vehicle_id spreads 100k vehicles evenly across shards; per-vehicle
// queries still target one shard because vehicle_id is in the shard key.
sh.enableSharding("fleet");
sh.shardCollection("fleet.telemetry", { vehicle_id: "hashed" });
sh.shardCollection("fleet.vehicle_state", { _id: "hashed" });
sh.shardCollection("fleet.alerts", { vehicle_id: "hashed" });
// Note: a unique index (message_id) on a sharded collection must include the shard key,
// so in production the idempotency index becomes { vehicle_id: 1, message_id: 1 }.
