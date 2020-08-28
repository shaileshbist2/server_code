const request = require('request');
const dynamicUsers = require('../models/DynamicUsers');
const serverDynamicConfig = require('../config/serverDynamicConfig');

class prodController {
    async isAlgoIdExists_in_pipeline_prod(params) {
        const options = {
            url: serverDynamicConfig.url_production + '/api/isAlgoIdExists_in_pipeline_prod',
            method: 'POST',
            headers: {
                'Accept': 'application/json'
            },
            json: true,
            body: params
        };
        return new Promise(resolve => {
            return request(options, function (err, response, body) {
                if (err) {
                    return resolve(err);
                } else {
                    return resolve(body);
                }
            });
        })

    }

    async checkStatus(params) {
        const dynamicModel = dynamicUsers(params.tableName);
        const process = await dynamicModel.findOne({ _id: params._id });
        if (process) {
            return { processStatus: process.processStatus }
        } else {
            return { processStatus: 'success' }
        }
    }

    async is_project_name_exist(params) {
        const options = {
            url: serverDynamicConfig.url_production + '/api/is_project_name_exist',
            method: 'POST',
            headers: {
                'Accept': 'application/json'
            },
            json: true,
            body: params
        };
        return new Promise(resolve => {
            return request(options, function (err, response, body) {
                if (err) {
                    return resolve(err);
                } else {
                    return resolve(body);
                }
            });
        })

    }

    async project(params) {
        const options = {
            url: serverDynamicConfig.url_production + '/api/project',
            method: 'POST',
            headers: {
                'Accept': 'application/json'
            },
            json: true,
            body: params
        };
        return new Promise(resolve => {
            return request(options, function (err, response, body) {
                if (err) {
                    return resolve(err);
                } else {
                    return resolve(body);
                }
            });
        })

    }

    async pipeline(params) {
        const options = {
            url: serverDynamicConfig.url_production + '/api/pipeline',
            method: 'POST',
            headers: {
                'Accept': 'application/json'
            },
            json: true,
            body: params
        };
        return new Promise(resolve => {
            return request(options, function (err, response, body) {
                if (err) {
                    return resolve(err);
                } else {
                    return resolve(body);
                }
            });
        })

    }
}

module.exports = new prodController();
