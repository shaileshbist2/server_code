const _ = require('lodash');
const rp = require('request-promise');
const exec = require('child_process').exec;
const staticConf = require('../config/serverStaticConfig.json');
const dynamicConf = require('../config/serverDynamicConfig.json');
const clientDynamicConf = require('../../client/config/dynamicConfig.json');
const clientStaticConf = require('../../client/config/staticConfig.json');
const devPipeline = require('../utils/devPipeline');
const servingPipeline = require('../utils/servingPipeline');
const esUtil = require('./esUtil');
const socket = require('./socketConfig')();
class appUtil {

    makeParamsForRunPipeline(params, serving, hiveUtil) {
        let body = {};
        body['processId'] = params.processId;
        body['timestamp'] = serving ? params.timestamp : new Date().getTime().toString();
        //body['elasticIndexRoi'] = 'roi' + Math.floor((Math.random() * 1000000) + 1);
        body['scheduling'] = 'false';
        body['pipelineId'] = params.username + '_' + params._id;
        body['userID'] = params.username;
        body['ensembling'] = params.ensembling;
        body['stacking'] = params.stacking;
        body['hiddenStoragePathForDP'] = clientDynamicConf.hiddenStoragePathForDP;
        body['hiveWarehouseDirName'] = dynamicConf.hiveWarehouseDirName;
        body['trainValidation'] = params.validateTrain.toString();
        body['deleteExistingPipeline'] = params.deleteExistingPath.toString();
        body['modelSaveOnWholeDataSetFlag'] = params.modelSaveOnWholeDataset.toString();
        body['sanityTest'] = params.sanityTest.toString();
        body['orderOfNAIComponents'] = params.orderOfNAIComponents;
        if (params.explorationES) {
            body['explorationES'] = params.explorationES;
            body['indexAndType'] = params.indexAndType;
            body['dataExplorationSettings'] = [];
            body['sampleSize'] = params.sampleSize;
        }
        body = Object.assign(body, dynamicConf.mysql, params.specialParamsToSendObj, params.sparkConf);
        if (this.findThirdPartyAlgoOrNot(params).length) {
            body["pythonScriptDataBaseName"] = dynamicConf.pythonScriptDataBaseName;
            body["pythonScriptLocalPath"] = dynamicConf.pythonScriptLocalPath;
        }
        if (params.interMediateVdSubmit) {
            params['vdProcessId'] = params.processId;
        }
        body = this.prepareRunJson(params, body, hiveUtil);
        body = this.processBody(body);
        return body;
    }

    apiCall(options, logShow) {
        if (logShow) {
            console.log('inpostttttttttttttttttttt', options.uri, options.body);
        }
        const p = new Promise(function (resolve, reject) {
            rp(options)
                .then(function (response) { resolve(response); })
                .catch(function (error) { reject(error); })
        });
        return p;
    }

    livyResponseHandleTilJobComplete(responseObj, params, _this, processStatusRes) {
        return new Promise((resolve, reject) => {
            var counter = 0;
            var a = setInterval(() => {
                counter = counter + 1;
                rp(dynamicConf.livyURI + '/batches/' + responseObj.id)
                    .then((resObj) => {
                        console.log('Livy Response: ', resObj);
                        resObj = typeof resObj !== 'object' ? JSON.parse(resObj) : resObj;
                        if (resObj.appInfo.driverLogUrl !== null && resObj.appInfo.driverLogUrl !== "null") {
                            this.updateProcessStatus(params, resObj, _this, processStatusRes);
                        }
                        if (resObj.state === 'success' || resObj.state === 'failed' || resObj.state === 'dead') {
                            clearInterval(a);
                            resObj.status = resObj.state;
                            this.updateProcessStatus(params, resObj, _this, processStatusRes);
                            if (!params.interMediateVdSubmit) {
                                this.updateAlgoStatus(params, resObj, _this);
                            }
                            resolve(resObj);
                        }
                    })
                    .catch((err) => {
                        clearInterval(a);
                        reject(err);
                    })
            }, 20000);
            if (counter > 20000000) {
                clearInterval(a);
                reject('error');
            }
        });
    }

    async handleResponse(req, body, options, initiateResponse, statusResponse) {
        let logResponse = await this.logGenerateCall(req, body, statusResponse);
        var thirdPartyAlgoExistArr = this.findThirdPartyAlgoOrNot(req);
        if (thirdPartyAlgoExistArr.length) {
            console.log(`script calll :+++++++++++++++ ${dynamicConf.scriptForThirdParyAlgo} ${dynamicConf.hdfsPath}${clientDynamicConf.hiddenStoragePathForDP} ${req.username}_${req.algoid} ${thirdPartyAlgoExistArr[0]["algoName"]} ${body.timestamp}`);
            exec(`${dynamicConf.scriptForThirdParyAlgo} ${dynamicConf.hdfsPath}${clientDynamicConf.hiddenStoragePathForDP} ${req.username}_${req.algoid} ${thirdPartyAlgoExistArr[0]["algoName"]} ${body.timestamp}`, function (error, stdout, stderr) {
                if (error) {
                    console.log(`script stderr : ${stderr}`);
                } else {
                    console.log(`script out :${stdout}`);
                }
                socket.sendNotification(req.username, statusResponse, req['processId']);
            })
            return;
        }
        if (body.interMediateVdSubmit) {
            statusResponse.uniqueColObj = await this.getUniqueValsEs(body);
        }
        socket.sendNotification(req.username, statusResponse, req['processId']);
    }

    updateProcessStatus(params, response, _this, processStatusRes) {
        let request = {
            tableName: 'process',
            processStatus: response.state,
            user: params.username,
            _id: processStatusRes._id,
            applicationId: response.appId,
            jobId: response.id
        }
        _this.saveOrUpdate(request);
    }

    async updateAlgoStatus(params, response, _this) {
        let request = {
            _id: params['algoid'],
            tableName: params.username,
            showResult: true
        }
        let showInServingFlag = await this.getFlagForServingShowPipeline(params, false, response);
        request.showInServingFlag = showInServingFlag;
        _this.saveOrUpdate(request);
    }

    prepareRunJson(params, body, hiveUtil) {
        if (params['pipelineAlgo'] === 'serving') {
            body['ElasticIndex'] = dynamicConf.servingIndex;
            body['WriteForROI'] = false;
            body = this.getServingJson(params, body, hiveUtil);
            return body;
        }
        body = this.getDevJson(params, body);
        return body;
    }

    getServingJson(params, body, hiveUtil) {
        body = servingPipeline.processServingPipeline(params, body, this, hiveUtil);
        return body;
    }

    async getDevJson(params, body) {
        body = await devPipeline.processDevPipeline(params, body, this);
        if (body.explorationES) {
            body.sinkSettings = this.processForAutomaticDex(body);
        }
        if (body.sourceSettings && Array.isArray(body.sourceSettings) && body.sourceSettings.length && body.sourceSettings[0].dataSource === 'facebookGraph') {
            body.sourceSettings[0].attributes = this.processForFaceBook(body.sourceSettings[0]);
        }
        return body;
    }

    prepareOptionsForHitApi(req, body) {
        return {
            method: 'POST',
            uri: `${dynamicConf.livyURI}/batches`,
            timeout: 30000000,
            headers: {
                'content-type': 'application/json'
            },
            body: this.getLivyObj(body, req),
            json: true
        };
    }

    getLivyObj(reqobj, req) {
        let livyObj = _.cloneDeep(dynamicConf.libyObj);
        if (typeof reqobj['driver-memory'] !== 'undefined') {
            livyObj.driverMemory = reqobj['driver-memory'];
            livyObj.executorCores = parseInt(reqobj['executor-cores']);
            livyObj.executorMemory = reqobj['executor-memory'];
            livyObj.numExecutors = parseInt(reqobj['num-executors']);
            if (reqobj['queue'] !== '') livyObj.queue = reqobj['queue'];
        }
        if (req.pipelineAlgo === 'serving') {
            livyObj.className = dynamicConf.servingJarClass;
            livyObj.file = dynamicConf.servingJarPath;
            livyObj.args[0] = 'modelServing';
            //livyObj.conf["spark.yarn.user.classpath.first"] = false;
        }
        if (!_.isEmpty(reqobj.jupyterSettings)) {
            livyObj.jars = [livyObj.file, dynamicConf.jupyterJar];
            livyObj.pyFiles = [dynamicConf.pyFiles];
            livyObj.file = ('/tmp/' + reqobj.jupyterSettings.FilePath).replace('ipynb', 'py');
            livyObj.args[0] = JSON.stringify(reqobj);
            delete livyObj.className;
            return livyObj;
        }
        reqobj = (req.pipelineAlgo === 'serving' ? reqobj : this.processForWasb(reqobj));
        livyObj.args.push(JSON.stringify(reqobj));
        if (req.pipelineAlgo === 'serving') {
            livyObj.args.push(JSON.stringify(this.devEditedJson))
        }
        return livyObj;
    }

    processForWasb(processObj) {
        processObj.sourceSettings = processObj.sourceSettings.map((obj) => {
            return this.checkWarb(obj)
        });
        for (let index in processObj.sinkSettings) {
            processObj.sinkSettings[index] = processObj.sinkSettings[index].map((obj) => {
                return this.sinkWarb(obj)
            });
        }
        processObj.ntContainerPath = dynamicConf.ntContainerPath;
        return processObj;
    }

    sinkWarb(obj) {
        if (obj.sinkType === 'wasb') {
            if (!obj.modelStorageOutputDir) obj.modelStorageOutputDir = obj.dfStorageOutputDir;
            delete obj.hdfsUri;
        }
        return obj;
    }

    checkWarb(obj) {
        if (obj.algo === 'Wasb') {
            obj.algo = "WASB";
            obj.dataSource = "wasb";
            obj.containerPath = "wasbs://" + obj.containerName + dynamicConf.wasbStore;
            obj.blobPath = '/' + obj.blobName;
            obj.updatedPath = "wasbs://" + obj.containerName + dynamicConf.wasbStore + obj.blobPath;
            delete obj.hdfsUri;
        }
        return obj;
    }

    processForFaceBook(data) {
        return data.attributes.replace(/{/g, '_Ob_').replace(/}/g, '_Cb_');
    }

    processForAutomaticDex(tempObj) {
        var obj = {
            "algo": "Elasticsearch",
            "esIndex": tempObj.indexAndType.split("/")[0],
            "esType": tempObj.indexAndType.split("/")[1],
            "esIp": tempObj.ElasticIP.replace("9300", "9200"),
            "Cluster": tempObj.ElasticCluster,
            "esNode": tempObj.ElasticClusterNode,
            "dataSource": "elastic",
            "esUser": "root",
            "esPassword": "root",
            "sinkType": "elasticsearch",
            "algoName": "ETLSink",
            "dragId": "",
            "algoText": "esSink",
            "hdfsUri": tempObj.commonHdfsUri
        };
        return [[obj]];
    }

    findThirdPartyAlgoOrNot(req) {
        return req.workflowRunState
            .filter(e => clientStaticConf.thirdPartyAlgo.includes(e.text));
    }

    processBody(body) {
        for (var key in body) {
            if (body[key] && typeof body[key] == 'object') {
                if (Array.isArray(body[key])) {
                    for (var i = 0; i < body[key].length; i++) {
                        if (typeof body[key][i] == 'string' && body[key][i].indexOf("{") > -1) {
                            body[key][i] = JSON.parse(body[key][i])
                        }
                    }
                }
            } else {
                if (body[key] && body[key].toString().indexOf('[') === -1 && body[key] && body[key].toString().indexOf('{') > -1) {
                    body[key] = [JSON.parse(body[key])]
                }
                if (body[key] && body[key].toString().indexOf('[') > -1 && typeof body[key] == 'string') {
                    body[key] = JSON.parse(body[key])
                }
            }
        }
        return body;
    }

    findEtlPresentOrNot(req) {
        var workflowRunState = req.workflowRunState;
        var index = _.findIndex(workflowRunState, { category: 'etl' });
        return index > -1 ? true : false;
    }

    makeOptions(method, uri, body, json, timeout = 30000, headers, token) {
        let options = { method, uri, body, json, timeout, headers }
        if (this.oauthFlag) {
            options.headers['Authorization'] = `Negotiate   ${token}`;
        }
        return options;
    }

    async getFlagForServingShowPipeline(params, showInServingFlag, response) {
        if (staticConf.pipelineSuccessStatus.includes(response.state)) {
            if (params.servingEnabledPipeline) {
                if (this.checkAlgoSpecificTag(params, showInServingFlag)) {
                    showInServingFlag = await this.checkStatusFromEs(params, showInServingFlag);
                    return showInServingFlag;
                }
                return showInServingFlag;
            }
            return showInServingFlag;
        }
        return showInServingFlag;
    }

    checkAlgoSpecificTag(params, showInServingFlag) {
        let algoParameters = params.algoParameters, val;
        Object.keys(algoParameters).some((key) => {
            val = algoParameters[key][staticConf.algoSpecificTagList[0]["algoName"]][staticConf.algoSpecificTagList[0]["tagName"]];
            return val;
        });
        if (typeof val === "undefined") val = "false"; //this case for if any algo which has no options of pmml yes or no
        return val ? staticConf.algoSpecificFlagForServing[staticConf.algoSpecificTagList[0]["tagName"]][val] : showInServingFlag;
    }

    async checkStatusFromEs(req, showInServingFlag) {
        let body = staticConf.bodyForBestModel;
        body.query.bool.must[0].match.pipelineId = `${req.user}_${req._id}`;
        body.query.bool.must[1].match.userId = req.user;
        body.query.bool.must[2].match.timestamp = req.timestamp;
        let response = await esUtil.getEsData({ index: dynamicConf.mysql.ElasticIndex, type: "_doc" }, body);
        if (response.error) {
            return showInServingFlag;
        }
        showInServingFlag = response.hits && response.hits.hits && response.hits.hits.length && response.hits.hits[0]._source.servingPipeLine;
        return showInServingFlag;
    }

    // cartesian product of arrays
    cartesianProductOf() {
        return _.reduce(arguments, function (a, b) {
            return _.flatten(_.map(a, function (x) {
                return _.map(b, function (y) {
                    return x.concat([y]);
                });
            }), false);
        }, [[]]);
    }

    async getUniqueValsEs(params) {
        let body = { "query": { "bool": { "must": [{ "match": { "VariableDerivationKey": params.vdProcessId } }, { "match": { "estype": "vdintermediatetype" } }] } } }
        let response = await esUtil.getEsData({ index: "vdintermediateindex", type: "_doc" }, body);
        if (response.error) {
            return { error: response.error, errorMsg: response.errorMsg };
        }
        let hits = response.data.hits.hits;
        if (!hits.length) {
            return {};
        }
        return hits[0]._source.UniqueValue;
    }

    async logGenerateCall(req, tempData, statusResponse) {
        let body = {
            applicationId: statusResponse.appId,
            timestamp: tempData.timestamp,
            ElasticIndex: dynamicConf.viewLogIndex,
            ElasticIP: dynamicConf.mysql.ElasticIP,
            ElasticCluster: dynamicConf.mysql.ElasticCluster,
            ElasticClusterNode: dynamicConf.mysql.ElasticClusterNode
        };
        let url = dynamicConf.viewLogFilterApi;
        let headers = { "content-type": "application/json" };
        let timeout = 30000;
        try {
            let options = this.makeOptions('POST', url, JSON.stringify(body), false, timeout, headers);
            let response = await this.apiCall(options, false);
            return response;
        } catch (error) {
            return {};
        }
    }

    async killApplicationlivy(query) {
        let url = `${dynamicConf.livyURI}/batches/${query.id}`;
        let headers = { 'content-type': 'application/json' };
        let timeout = 300000;
        let options = this.makeOptions('DELETE', url, JSON.stringify(query), false, timeout, headers);
        let killResopnse = await this.apiCall(options, false);
        return killResopnse;
    }

}
module.exports = new appUtil();