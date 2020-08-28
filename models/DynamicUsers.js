const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const dynamicUserSchema = new Schema({},{versionKey : false,strict: false});
let dynamicModels = {};

const dynamicModel = (collectionName) => {
    if( !(collectionName in dynamicModels) ){
        dynamicModels[collectionName] = mongoose.model(collectionName, dynamicUserSchema, collectionName);
    }
    return dynamicModels[collectionName];
};
module.exports = dynamicModel;