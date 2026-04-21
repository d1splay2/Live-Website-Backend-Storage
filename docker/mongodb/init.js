const database = 'data';
const collection = 'persons';

db = db.getSiblingDB(database);

db.createCollection(collection);

db.createUser({
    user: process.env.SPARK_USER_USERNAME,
    pwd: process.env.SPARK_USER_PASSWORD,
    roles: [{ role: 'readWrite', db: database }]
});

db.createUser({
    user: process.env.PYMONGO_USER_USERNAME,
    pwd: process.env.PYMONGO_USER_PASSWORD,
    roles: [{ role: 'read', db: database }]
});
