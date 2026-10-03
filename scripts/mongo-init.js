// Runs once on first start of the MongoDB container.
// Creates a least-privilege application user (readWrite on the fleet DB only).
const appUser = process.env.MONGO_APP_USER || 'fleet_app';
const appPwd = process.env.MONGO_APP_PASSWORD || 'fleet-app-dev-password';
const fleet = db.getSiblingDB('fleet');
fleet.createUser({ user: appUser, pwd: appPwd, roles: [{ role: 'readWrite', db: 'fleet' }] });
fleet.createCollection('telemetry');
fleet.createCollection('alerts');
fleet.createCollection('vehicle_state');
fleet.createCollection('analytics_snapshots');
print('fleet database initialised for ' + appUser);
