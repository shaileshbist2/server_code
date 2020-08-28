const dynamicUsers = require('../models/DynamicUsers');
const appUtil = require('../utils/appUtil');
const socket = require('../utils/socketConfig')();
const dynamicConf = require('../config/serverDynamicConfig.json');
const hdfsUtil = require('../utils/hdfsUtil');
let hiveUtil = require('../utils/hiveUtil');
hiveUtil = new hiveUtil;
const _ = require('lodash');
const fs = require('fs');
const exec = require('child_process').exec;

class pipelineController {

    async saveOrUpdate(params) {
        const dynamicModel = dynamicUsers(params.tableName);
        let response = {};
        if (!params._id) {
            const doc = new dynamicModel(params);
            if (params.hasOwnProperty('check')) {
                if (params.check === 'project') {
                    const project = await dynamicModel.findOne({ projectName: params.projectName });
                    if (project) {
                        response = 'project_exists';
                    } else {
                        response = await doc.save();
                    }
                } else if (params.check === 'pipeline') {
                    const pipeline = await dynamicModel.findOne({ algoName: params.algoName });
                    if (pipeline) {
                        response = 'pipeline_exists';
                    } else {
                        response = await doc.save();
                    }
                }
            } else {
                response = await doc.save();
            }
        } else {
            const filter = { _id: params._id };
            const update = params;
            response = await dynamicModel.findOneAndUpdate(filter, update);
        }
        return response;
    }

    async runPipeline(params) {
        params = await this.reverseColumnProcess(params);
        const body = await appUtil.makeParamsForRunPipeline(params, false, hiveUtil);
        const options = appUtil.prepareOptionsForHitApi(params, body);
        if (!body.interMediateVdSubmit) {
            const timestampUpdateStatus = await this.updateTimestamp(params, body);
        }
        const processStatusRes = await this.createProcess(params, body);
        try {
            if (!_.isEmpty(body.jupyterSettings)) {
                await this.putfileOnServer(body);
            }
            const initiateResponse = await appUtil.apiCall(options, true);
            try {
                const statusResponse = await appUtil.livyResponseHandleTilJobComplete(initiateResponse, params, this, processStatusRes);
                appUtil.handleResponse(params, body, options, initiateResponse, statusResponse);
            } catch (statusError) {
                //console.log("statusError ===================", statusError);
                appUtil.updateProcessStatus(params, { state: 'failed' }, this, processStatusRes);
            }
        } catch (initiateError) {
            //console.log("initiateError ======================", initiateError);
            appUtil.updateProcessStatus(params, { state: 'service unavailable' }, this, processStatusRes);
            socket.sendNotification(params.username, { error: true, message: initiateError.message }, body['processId']);
        }
    }

    async createProcess(params, body) {
        let request = {
            tableName: 'process',
            processStatus: 'start',
            timestamp: body.timestamp,
            applicationId: 'NA',
            message: '',
            pipelineAlgoId: params.algoid,
            workspaceName: params.workspaceName,
            workspace: params.workspace,
            projectName: params.projectName,
            pipelineName: params.algoName,
            pipelineId: params.username + '_' + params.algoid
        }
        const processStatusRes = await this.saveOrUpdate(request);
        return processStatusRes;
    }

    async updateTimestamp(params, body) {
        let request = {
            _id: params._id,
            tableName: params.username,
            timestamp: body.timestamp
        }
        const updatedTimestampStatus = await this.saveOrUpdate(request);
        return updatedTimestampStatus;
    }

    async list(params) {
        const offset = params.offset || 0;
        const limit = params.limit || 0;
        const searchText = params.searchText || '';
        const searchKey = params.searchKey || '';
        const projectId = params.projectId || '';
        const dynamicModel = dynamicUsers(params.tableName);
        let query;
        if (searchKey) {
            if (projectId) {
                query = { $and: [{ tableName: params.tableName }, { projectId: projectId }], [searchKey]: { $regex: searchText, $options: "i" } };
            } else {
                query = { $and: [{ user: params.user }], [searchKey]: { $regex: searchText, $options: "i" } };
            }
            const data = await dynamicModel.find(query).skip(offset).sort({ updated_on: -1 }).limit(limit);
            const total = await dynamicModel.find(query);
            return {
                ...params,
                total: total.length,
                data: data
            };
        } else {
            const sort = params.sort || { _id: -1 };
            const data = await dynamicModel.find({ ...params.query }, { ...params.projection }).skip(offset).sort(sort).limit(limit);
            return data;
        }
    }

    async readHdfsData(request) {
        let hdfsData = {}, token = '';
        if (dynamicConf.oauthFlag) {
            [dynamicConf.host, dynamicConf.deactiveHost, dynamicConf.kerberosConfig.hostbasedService, token] = await new hdfsUtil(dynamicConf, request).findActiveNode(request);
        }
        try {
            hdfsData = await new hdfsUtil(dynamicConf, request).getData(dynamicConf, request, token);
            return hdfsData;
        } catch (error) {
            throw error
        }
    }

    async getPipelineById(query) {
        const dynamicModel = dynamicUsers(query.tableName);
        const response = await dynamicModel.find({ _id: query.pipelineId });
        return response;
    }

    async getPipelineByKey(query) {
        const dynamicModel = dynamicUsers(query.tableName);
        const response = await dynamicModel.find(query).sort({ _id: -1 });
        return response;
    }

    async deletePipeline(params) {
        const dynamicModel = dynamicUsers(params.tableName);
        let response = {};
        if (params._id) {
            const deletePipelin = { _id: params._id };
            const update = params;
            response = await dynamicModel.deleteOne(deletePipelin, update);
        }
        return response;
    }

    async deleteProject(params) {
        let response = { error: false };
        let userTableName = params.userTableName;
        let projectTable = params.appProjectTableName;
        let id = params.appProjectId;
        let projectName = params.projectName;
        let userModel = dynamicUsers(userTableName);
        let projectModel = dynamicUsers(projectTable);
        try {
            response = await userModel.deleteMany({ projectId: id, projectName: projectName });
            response = await projectModel.deleteOne({ _id: id });
        } catch (err) {
            response.error = true;
            response.message = err;
        }
        return response;
    }

    async readHive(params) {
        if (typeof params.hiveTablesList == 'string' && params.hiveTablesList !== '') {
            var tempStr = params.hiveTablesList;
            params.hiveTablesList = [];
            params.hiveTablesList.push(tempStr);
        }
        if (!params.readDataFromParams) {
            params.hiveServerMachineIP = dynamicConf.hiveConfig.hiveServerMachineIP;
            params.hiveServerMachinePort = dynamicConf.hiveConfig.hiveServerMachinePort;
            params.hiveUsername = dynamicConf.hiveConfig.hiveUsername;
            params.hivePassword = dynamicConf.hiveConfig.hivePassword;
            params.limitSize = dynamicConf.hiveConfig.limitSize;
        } else if (params.readDataFromParams) {
            params.hiveServerMachineIP = dynamicConf.hiveConfig.hiveServerMachineIP;
            params.hiveServerMachinePort = dynamicConf.hiveConfig.hiveServerMachinePort;
        }
        var url = '';
        if (params.hiveJoinTableNameColumnList !== '{}' && typeof params.hiveJoinTableNameColumnList === 'string') {
            params.hiveJoinTableNameColumnList = JSON.parse(params.hiveJoinTableNameColumnList);
            params.hiveMachineIp = params.hiveServerMachineIP;
            params.hiveMachinePort = params.hiveServerMachinePort;
            delete params.hiveServerMachineIP;
            delete params.hiveServerMachinePort;
            delete params.limitSize;
            delete params.hiveTablesList;
            delete params.hiveDB;
            delete params.hiveTableName;
        }
        var hostIps = dynamicConf.hiveConfig.hiveIpConfig.hosts;
        var hiveUpdated = false;
        var nameNode = '';
        hostIps.forEach(function (_ob) {
            if (req.hostname == _ob.ip) {
                hiveUpdated = true;
                nameNode = _ob.nameNode
            }
        })
        if (hiveUpdated) {
            if (params.process === 'show databases') {
                url = 'http://' + nameNode + ':8090/NDPWrappper-0.0.1-SNAPSHOT/ndpservice/get_database/';
            } else if (params.process === 'show tables') {
                url = 'http://' + nameNode + ':8090/NDPWrappper-0.0.1-SNAPSHOT/ndpservice/get_table/';
            } else if (params.process === 'show columns') {
                url = 'http://' + nameNode + ':8090/Yottaone/yottaone/gettablescolumns/';
            } else if (params.process === 'show view') {
                url = 'http://' + nameNode + ':8090/NDPWrappper-0.0.1-SNAPSHOT/ndpservice/get_rows/';
            } else if (params.process === 'drive variables') {
                url = 'http://' + nameNode + ':8090/MachinfinityDataPreparation/machinfinitydataprep/hivevariablederivation/';
            } else if (params.process === 'model table') {
                url = 'http://' + nameNode + ':8090/NDPWrappper-0.0.1-SNAPSHOT/ndpservice/searchTables/';
                params.hiveDatabaseIP = params.hiveServerMachineIP;
                params.searchString = params.hiveTableName;
            }
            delete params.process;
            appUtilServer.getPostRequestPromise(url, params).then(function (response) {
                res.send(response);
            });
        } else {
            // as usaual case
            if (params.process === 'show databases') {
                const response = await hiveUtil.getdatabases(params);
                return response.responseData;
                // hiveUtil.getdatabases(params).then(function (response) {
                //     res.send(response);
                // })
            } else if (params.process === 'show tables') {
                const response = await hiveUtil.gettables(params);
                return response.responseData;
                // hiveUtil.gettables(params).then(function (response) {
                //     res.send(response);
                // })
            } else if (params.process === 'show columns') {
                const response = await hiveUtil.gettablescolumns(params);
                return response.responseData;
                // hiveUtil.gettablescolumns(params).then(function (response) {
                //     res.send(response);
                // })
            } else if (params.process === 'show view') {
                const response = await hiveUtil.viewtabledata(params)
                return response.responseData;
                // hiveUtil.viewtabledata(params).then(function (response) {
                //     res.send(response);
                // })
            } else if (params.process === 'model table') {
                const response = await hiveUtil.modeltabledata(params);
                return response.responseData;
                // hiveUtil.modeltabledata(params).then(function (response) {
                //     res.send(response);
                // })
            } else if (params.process === 'drive variables') {
                url = 'http://localhost:8090/MachinfinityDataPreparation/machinfinitydataprep/hivevariablederivation/';
            }
        }
    }

    async reverseColumnProcess(params) {
        let reverseProcessIndex = _.findIndex(params.workflowRunState, { text: 'hive' });
        if (reverseProcessIndex > -1) {
            params = await this.modifyParams(params);
        }
        return params;
    }

    async modifyParams(params) {
        let etlPresent = appUtil.findEtlPresentOrNot(params);
        for (let source in params.algoParameters) {
            for (let algo in params.algoParameters[source]) {
                if (algo === 'hive') {
                    if (params.algoParameters[source][algo].hiveListOfCols) {
                        let query = {
                            hiveDB: params.algoParameters[source][algo].hiveDatabase,
                            hiveTableName: params.algoParameters[source][algo].hiveTable,
                            process: "show columns"
                        }
                        let allCols = await this.readHive(query);
                        let nonFeatureCols = params.algoParameters[source][algo].hiveListOfCols;
                        params.algoParameters[source][algo].hiveListOfCols = this.assignReverseValue(allCols, etlPresent, nonFeatureCols, params.algoParameters[source][algo]);
                    }
                }
            }
        }
        return params;
    }

    async paginate_list(params) {
        const offset = params.offset || 0;
        const limit = params.limit || 0;
        const searchKey = params.searchKey || '';
        const dynamicModel = dynamicUsers(params.tableName);
        if (searchKey) {
            const data = await dynamicModel.find(params.query).skip(offset).sort({ updated_on: -1 }).limit(limit);
            const total = await dynamicModel.find(params.query);
            return {
                ...params,
                total: total.length,
                data: data
            };
        } else {
            const sort = params.sort || { _id: -1 };
            const data = await dynamicModel.find({ ...params.query }).skip(offset).sort(sort).limit(limit);
            return data;
        }
    }

    assignReverseValue(allCols, etlPresent, nonFeatureCols, params) {
        if (etlPresent) {
            let sourceName = `${params.sourceName}_`;
            let nonFeatureColsWithoutSrc = nonFeatureCols.split(',').map((e) => e.replace(sourceName, ""));
            nonFeatureCols = _.differenceWith(allCols.split(','), nonFeatureColsWithoutSrc, _.isEqual);
            nonFeatureCols = nonFeatureCols.map(e => `${sourceName}${e}`).join(",");
            return nonFeatureCols;
        }
        nonFeatureCols = _.differenceWith(allCols.split(','), nonFeatureCols.split(','), _.isEqual);
        nonFeatureCols = nonFeatureCols.join(',');
        return nonFeatureCols;
    }

    async createFile(params) {
        const path = '/executionFiles/';
        const file = params.file;
        if (!fs.existsSync(`${dynamicConf.jupyterNotebookPath}${path}`)) {
            fs.mkdirSync(`${dynamicConf.jupyterNotebookPath}${path}`);
        }
        const codeobj = {
            "type": "notebook",
            "content": {
                "metadata": {
                    "kernelspec": { "display_name": "Python 3", "language": "python", "name": "python3" },
                    "language_info": {
                        "codemirror_mode": { "name": "ipython", "version": 3 },
                        "file_extension": ".py",
                        "mimetype": "text/x-python",
                        "name": "python",
                        "nbconvert_exporter": "python",
                        "pygments_lexer": "ipython3",
                        "version": "3.6.7"
                    }
                },
                "nbformat": 4,
                "nbformat_minor": 0,
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": "import sys \nfrom pyspark.sql import SparkSession, DataFrame, SQLContext \nimport naipysparkcustomizedModule \n\nif __name__ == '__main__': \n # common code to invoke NAI\n  jsonForNai = sys.argv[1] \n  print(\"json for nai is : \")\n  print(jsonForNai)\n# in pipeline\n\n  dfList= naipysparkcustomizedModule.getDataFrame(jsonForNai)\n\n# start writing code below using dfList \n\n  naipysparkcustomizedModule.setDataFrame(dfList) \n\n# to ouput dataframe list after transformation use naipysparkcustomizedModule.setDataFrame(<result Df List>)\n ",
                        "outputs": [],
                        "execution_count": 0
                    }
                ]
            }
        }
        const folderpath = `${path}${file}`;
        if (!fs.existsSync(`${dynamicConf.jupyterNotebookPath}${path}${file}`)) {
            let options = appUtil.makeOptions('PUT', `${dynamicConf.jupyterUrl}/api/contents${folderpath}`, JSON.stringify(codeobj), false, 3000000, { "content-type": "application/json" });
            let response = await appUtil.apiCall(options, false);
            return response;
        }
        return { status: 'allreadyExist' };
    }

    async putfileOnServer(body) {
        const hdfsPath = `/tmp/${body.jupyterSettings.FilePath}`;
        const path = '/executionFiles/';
        const localpath = `${dynamicConf.jupyterNotebookPath}${path}${body.jupyterSettings.FilePath}`;
        this.convertToPy(localpath).then((res) => {
            new hdfsUtil(dynamicConf, {}).copyFileInHdfs(localpath.replace('ipynb', 'py'), hdfsPath.replace('ipynb', 'py'));
        });
    }

    convertToPy(localpath) {
        return new Promise((resolve) => {
            let cmd = `${dynamicConf.jupyterExec} nbconvert --to script ${localpath}`;
            exec(cmd, function (error, stdout, stderr) {
                console.log(error, stdout, stderr);
                resolve();
            });
        });
    }
}

module.exports = new pipelineController();
