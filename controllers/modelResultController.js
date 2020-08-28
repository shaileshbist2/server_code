const staticConf = require('../config/serverStaticConfig.json');
const clientStaticConf = require('../../client/config/staticConfig.json');
const dynamicConf = require('../config/serverDynamicConfig.json');
const esUtil = require('../utils/esUtil');
const _ = require('lodash');

class modelResultController {

    async getModelData(request) {
        let resultSets = [];
        try {
            let data = await this.readEachTypeOfData(request, resultSets);
            console.log('Es Info : ', JSON.stringify(data));
            data = this.getContentPrepare(data);
            return data;
        } catch (error) {
            console.log('Error :', error);
            throw error;
        }
    }

    async readEachTypeOfData(request, resultSets) {
        if (!request.types.length) {
            return new Promise((resolve, reject) => {
                if (Array.isArray(resultSets)) {
                    return resolve(resultSets)
                }
                return reject(resultSets);
            })
        }
        let estypeObj = request.types.shift();
        let temp = await this.readEsContent(request, estypeObj, resultSets);
        return this.readEachTypeOfData(request, temp);
    }

    async readEsContent(request, estypeObj, resultSets) {

        let body = this.getBody(request, estypeObj), tempResult;
        console.log(request.index + "  " + request.type + " " + JSON.stringify(body));
        tempResult = await esUtil.getEsData(request, body);
        if (tempResult.error) {
            return tempResult.errorMsg;
        }
        if (request.dynamicConnectionFlag) {
            request = this.getTypeForDynamicConnections(request);
        }
        resultSets.push({ [request.estype]: tempResult.data.hits.hits });
        return resultSets;
    }

    getTypeForDynamicConnections(request) {
        if (clientStaticConf.types.timeSeries.includes(request.option)) {
            request.estype = 'timeSeriesType';
        }
        if (clientStaticConf.types.classification.includes(request.option)) {
            request.estype = 'binaryClassificationType';
        }
        if (clientStaticConf.types.regression.includes(request.option)) {
            request.estype = 'RegressionType';
        }
        return request
    }

    getBody(request, estypeObj) {
        if (!_.isEmpty(estypeObj)) {
            request.estype = estypeObj.estype;
            request.branchId = estypeObj.branchId;
        }
        return {
            size: staticConf.sizeForEsData,
            query: {
                query_string: {
                    query: this.getQueryString(request)
                }
            }
        }
    }

    getQueryString(req) {
        let queryStr = '';
        staticConf.fieldsFilter.forEach((elm) => {
            if (!queryStr && elm !== 'timestamp' && req[elm]) queryStr += elm + " : " + "'" + req[elm] + "'";
            else if (queryStr && elm !== 'timestamp' && req[elm]) queryStr += " AND " + elm + " : " + "'" + req[elm] + "'";
            else if (req[elm]) queryStr += " AND " + elm + " : " + req[elm];
        })
        return queryStr;
    }

    getContentPrepare(data) {
        var _this = this;
        var rootType = [];
        var childType = [];
        var timeSeriesType = [];
        var header = [];
        var row = [];
        var algoName = '';
        var clusterCenters = [];
        var clusterIndex = '';
        var confusionMetricData = [['', 'Condition Positive', 'Condition Negative'], ['Prediction Positive'], ['Prediction Negative']];
        var branchPathArray = [];
        var branchPath = '';
        var coefficientHeaders = [];
        var coefficientValues = [];
        var type = '';
        var confusionMetricDataMulti = [];
        var value = [];
        var labelCorrespondMetricData = [];
        var labelCorrespondMetricHeader = [];
        var hyperParameterTunning = '';
        var bestModelHyperParams = '';
        var bestModelHyperParamsHeader = [];
        var bestModelHyperParamsValues = [];
        var nextPredictionHeader = '';
        var nextPredictionRow = '';
        var decileDataHeader = [];
        var decileDataRow = [];
        var liftPoints = [];
        var gainPoints = [];
        var rocPoint = [];
        var ksTestJson = [];
        var lorenzJson = [];
        var featureImportance = {};
        var elbowPoints = [];
        var conditionPostive = '';
        var conditionNegative = '';
        var bestModel = 'false';
        var autoMl = false;
        var segmentData = {};
        var segmentFlag = false;
        data.forEach(function (dat) {
            for (var key in dat) {
                if (key === 'ClusteringType') {
                    dat[key].forEach(function (cluster) {
                        for (var clKey in cluster['_source']) {
                            if (clKey != 'userId' &&
                                clKey != 'pipelineId' &&
                                clKey != 'branchId' &&
                                clKey != 'algoName' &&
                                clKey != 'algoType' &&
                                clKey != 'branchPath' &&
                                clKey != 'hyperParameterTunning' &&
                                clKey != 'bestModelHyperParams' &&
                                clKey != 'clusterCenters' &&
                                clKey != 'elbowPoints' &&
                                clKey != 'timeStampTypeD' &&
                                clKey != 'projectId' &&
                                clKey != 'algoinput' &&
                                clKey != 'estype' &&
                                clKey != 'indexOrder'
                            ) {
                                if (cluster['_source'][clKey] !== "NA" && cluster['_source'][clKey] !== "N/A") {
                                    header.push(clKey);
                                    row.push(cluster['_source'][clKey]);
                                }
                            }
                            if (clKey == 'algoName') {
                                algoName = cluster['_source'][clKey]
                                type = "Clustering"
                            }
                            if (clKey == 'clusterCenters') {
                                var clusters = cluster['_source'][clKey];
                                if (clusters.match(/:/)) {
                                    clusters = clusters.split(':');
                                    for (var i = 0; i < clusters.length; i++) {
                                        clusterCenters.push(clusters[i].split(','));
                                    }
                                } else {
                                    clusterCenters.push(clusters.split(','));
                                }
                                type = "Clustering";
                            }
                            if (clKey == 'hyperParameterTunning') {
                                hyperParameterTunning = cluster['_source'][clKey]
                            }
                            if (clKey == 'bestModelHyperParams') {
                                bestModelHyperParams = cluster['_source'][clKey]
                                if (bestModelHyperParams && bestModelHyperParams.indexOf(',') > -1) {
                                    bestModelHyperParams = bestModelHyperParams.split(',');
                                    for (var i = 0; i < bestModelHyperParams.length; i++) {
                                        if (bestModelHyperParams[i].indexOf('::')) {
                                            var str = bestModelHyperParams[i].split('::');
                                            bestModelHyperParamsHeader.push(str[0]);
                                            bestModelHyperParamsValues.push(str[1]);
                                        }
                                    }
                                }
                            }
                            if (clKey === 'elbowPoints') {
                                elbowPoints = cluster['_source'][clKey];
                            }
                            if (clKey === 'bestModel') {
                                bestModel = cluster['_source'][clKey];
                            }
                        }

                        [header, row] = _this.checkForHeaderAndRowChange(cluster['_source'], header, row);

                        obj = {
                            header: header,
                            row: row,
                            algoName: algoName,
                            clusterCenters: clusterCenters,
                            hyperParameterTunning: hyperParameterTunning,
                            bestModelHyperParamsHeader: bestModelHyperParamsHeader,
                            bestModelHyperParamsValues: bestModelHyperParamsValues,
                            elbowPoints: elbowPoints,
                            bestModel: bestModel
                        }
                        rootType.push(obj);
                        header = [];
                        row = [];
                        clusterCenters = [];
                        hyperParameterTunning = '';
                        bestModelHyperParams = '';
                        bestModelHyperParamsHeader = [];
                        bestModelHyperParamsValues = [];
                        elbowPoints = [];
                        bestModel = 'false';
                    })
                }
                if (key === 'binaryClassificationType') {
                    dat[key].forEach(function (classification) {
                        for (var clk in classification['_source']) {
                            if (clk != 'userId' &&
                                clk != 'pipelineId' &&
                                clk != 'branchId' &&
                                clk != 'algoName' &&
                                clk != 'clusterIndex' &&
                                clk != 'algoType' &&
                                clk != 'branchPath' &&
                                clk != 'hyperParameterTunning' &&
                                clk != 'bestModelHyperParams' &&
                                clk != 'classificationType' &&
                                clk != 'truePostive' &&
                                clk != 'trueNegative' &&
                                clk != 'falsePostive' &&
                                clk != 'falseNegative' &&
                                clk != 'conditionPostive' &&
                                clk != 'conditionNegative' &&
                                clk != 'DecileHeaders' &&
                                clk != 'timeStampTypeD' &&
                                clk != 'algoinput' &&
                                clk != 'ResponsePercentages' &&
                                clk != 'LiftPoints' &&
                                clk != 'ksTestJson' &&
                                clk != 'lorenzJson' &&
                                clk != 'featureImportance' &&
                                clk != 'GainPoints' &&
                                clk != 'RocPoints' &&
                                clk != 'projectId' &&
                                clk != 'estype' &&
                                clk != 'indexOrder'
                            ) {
                                if (classification['_source'][clk] !== "NA" && classification['_source'][clk] !== "N/A") {
                                    header.push(clk);
                                    row.push(classification['_source'][clk]);
                                }
                            }
                            if (clk == 'branchPath') {
                                branchPath = classification['_source'][clk];
                                type = 'Classification';
                            }
                            if (clk == 'clusterIndex') {
                                clusterIndex = classification['_source'][clk]
                            }
                            if (clk == 'truePostive') {
                                //confusionMetricData[1].push(classification['_source'][clk]);
                                confusionMetricData[1][1] = classification['_source'][clk];
                            }
                            if (clk == 'trueNegative') {
                                confusionMetricData[2][2] = classification['_source'][clk];
                            }
                            if (clk == 'falseNegative') {
                                confusionMetricData[2][1] = classification['_source'][clk];
                            }
                            if (clk == 'falsePostive') {
                                confusionMetricData[1][2] = classification['_source'][clk];
                            }
                            if (clk == 'hyperParameterTunning') {
                                hyperParameterTunning = classification['_source'][clk]
                            }
                            if (clk == 'bestModelHyperParams') {
                                bestModelHyperParams = classification['_source'][clk]
                                if (bestModelHyperParams && bestModelHyperParams.indexOf(',') > -1) {
                                    bestModelHyperParams = bestModelHyperParams.split(',');
                                    for (var i = 0; i < bestModelHyperParams.length; i++) {
                                        if (bestModelHyperParams[i].indexOf('::')) {
                                            var str = bestModelHyperParams[i].split('::');
                                            bestModelHyperParamsHeader.push(str[0]);
                                            bestModelHyperParamsValues.push(str[1]);
                                        }
                                    }
                                }
                            }
                            if (clk == 'DecileHeaders') {
                                decileDataHeader = classification['_source'][clk].split(',');
                            }
                            if (clk == 'ResponsePercentages') {
                                decileDataRow = classification['_source'][clk].split(',');
                            }
                            if (clk == 'LiftPoints') {
                                liftPoints = classification['_source'][clk];
                            }
                            if (clk == 'ksTestJson') {
                                ksTestJson = classification['_source'][clk];
                            }
                            if (clk == 'lorenzJson') {
                                lorenzJson = classification['_source'][clk];
                            }
                            if (clk == 'GainPoints') {
                                gainPoints = classification['_source'][clk];
                            }
                            if (clk == 'RocPoints') {
                                rocPoint = classification['_source'][clk];
                            }
                            if (clk == 'featureImportance') {
                                featureImportance = classification['_source'][clk];
                            }
                            if (clk === 'conditionPostive') {
                                conditionPostive = classification['_source'][clk];
                            }
                            if (clk === 'conditionNegative') {
                                conditionNegative = classification['_source'][clk];
                            }
                            if (clk === 'bestModel') {
                                bestModel = classification['_source'][clk];
                            }
                            if (clk === 'algoType' && classification['_source'][clk] === "Automl-c") {
                                autoMl = true;
                            }
                        }
                        // processing for sort decile value
                        var tempDecileArr = [];
                        var finalDecileArr = [];
                        for (var decileIndex = 0; decileIndex < decileDataRow.length; decileIndex++) {
                            var tempdecileObj = {};
                            tempdecileObj[decileDataHeader[decileIndex]] = decileDataRow[decileIndex];
                            tempDecileArr.push(tempdecileObj);
                        }
                        for (var decileIndexFinal = 0; decileIndexFinal < tempDecileArr.length; decileIndexFinal++) {
                            for (var decileKe in tempDecileArr[decileIndexFinal]) {
                                var pushIndex = parseInt(decileKe);
                                finalDecileArr[pushIndex - 1] = tempDecileArr[decileIndexFinal][decileKe];
                            }
                        }

                        // modify confusionMetricData with condition postive and negative value
                        for (var ii = 0; ii < confusionMetricData.length; ii++) {
                            for (var jj = 0; jj < confusionMetricData[ii].length; jj++) {
                                if (confusionMetricData[ii][jj] === 'Prediction Positive') confusionMetricData[ii][jj] = confusionMetricData[ii][jj] + '(' + conditionPostive + ')';
                                if (confusionMetricData[ii][jj] === 'Prediction Negative') confusionMetricData[ii][jj] = confusionMetricData[ii][jj] + '(' + conditionNegative + ')';
                                if (confusionMetricData[ii][jj] === 'Condition Positive') confusionMetricData[ii][jj] = confusionMetricData[ii][jj] + '(' + conditionPostive + ')';
                                if (confusionMetricData[ii][jj] === 'Condition Negative') confusionMetricData[ii][jj] = confusionMetricData[ii][jj] + '(' + conditionNegative + ')';
                            }
                        }

                        obj = {
                            branchPath: branchPath,
                            type: type,
                            content: [{
                                header: header,
                                row: row,
                                clusterIndex: clusterIndex,
                                confusionMetricData: autoMl ? [] : confusionMetricData,
                                labelCorrespondMetric: [],
                                hyperParameterTunning: hyperParameterTunning,
                                bestModelHyperParamsHeader: bestModelHyperParamsHeader,
                                bestModelHyperParamsValues: bestModelHyperParamsValues,
                                decileDataRow: finalDecileArr,
                                liftPoints: liftPoints,
                                gainPoints: gainPoints,
                                ksTestJson: ksTestJson,
                                lorenzJson: lorenzJson,
                                rocPoint: rocPoint,
                                featureImportance: featureImportance,
                                bestModel: bestModel
                            }
                            ]
                        }
                        childType.push(obj);
                        header = [];
                        row = [];
                        hyperParameterTunning = '';
                        bestModelHyperParams = '';
                        confusionMetricData = [['', 'Condition Positive', 'Condition Negative'], ['Prediction Positive'], ['Prediction Negative']];
                        bestModelHyperParamsHeader = [];
                        bestModelHyperParamsValues = [];
                        decileDataHeader = [];
                        decileDataRow = [];
                        liftPoints = [];
                        ksTestJson = [];
                        lorenzJson = [];
                        gainPoints = [];
                        rocPoint = [];
                        featureImportance = {};
                        bestModel = 'false';
                        autoMl = false;
                    })
                }
                if (key === 'multinomialClassificationType') {
                    dat[key].forEach(function (classification) {
                        for (var clk in classification['_source']) {
                            if (clk != 'userId' &&
                                clk != 'pipelineId' &&
                                clk != 'branchId' &&
                                clk != 'algoName' &&
                                clk != 'clusterIndex' &&
                                clk != 'algoType' &&
                                clk != 'branchPath' &&
                                clk != 'hyperParameterTunning' &&
                                clk != 'bestModelHyperParams' &&
                                clk != 'classificationType' &&
                                clk != 'confusionMetric' &&
                                clk != 'labelCorrespondMetric' &&
                                clk != 'DecileHeaders' &&
                                clk != 'ResponsePercentages' &&
                                clk != 'algoinput' &&
                                clk != 'featureImportance' &&
                                clk != 'timeStampTypeD' &&
                                clk != 'projectId' &&
                                clk != 'estype' &&
                                clk != 'indexOrder'
                            ) {
                                if (classification['_source'][clk] !== "NA" && classification['_source'][clk] !== "N/A") {
                                    header.push(clk);
                                    row.push(classification['_source'][clk]);
                                }
                            }
                            if (clk == 'branchPath') {
                                branchPath = classification['_source'][clk];
                                type = 'Classification';
                            }
                            if (clk == 'clusterIndex') {
                                clusterIndex = classification['_source'][clk]
                            }
                            if (clk == 'confusionMetric') {
                                classification['_source'][clk].forEach(function (indx) {
                                    confusionMetricDataMulti.push(indx.rowValues.split(','));
                                })
                            }
                            if (clk == 'labelCorrespondMetric') {
                                value = [];
                                var t = [];
                                for (key in classification['_source'][clk][0]) {
                                    if (key != 'FMeasure') {
                                        labelCorrespondMetricHeader.push(key);
                                    }
                                }
                                for (key in classification['_source'][clk][0]) {
                                    if (key == 'FMeasure') {
                                        labelCorrespondMetricHeader.push(key);
                                    }
                                }
                                classification['_source'][clk].forEach(function (obj, index) {
                                    for (var key in obj) {
                                        if (key != 'Label' && key != 'FMeasure') {
                                            t.push(obj[key]);
                                        }
                                    }
                                    value.push(t);
                                    t = [];
                                });
                                t = [];
                                var i = 0;
                                classification['_source'][clk].forEach(function (obj, index) {
                                    for (var key in obj) {
                                        if (key == 'FMeasure') {
                                            value[i].push(obj[key]);
                                        }
                                    }
                                    i++;
                                });
                                var i = 0;
                                classification['_source'][clk].forEach(function (obj, index) {
                                    for (var key in obj) {
                                        if (key == 'Label') {
                                            value[i].unshift(obj[key]);
                                        }
                                    }
                                    i++
                                });
                                value.unshift(labelCorrespondMetricHeader);
                            }
                            if (clk == 'hyperParameterTunning') {
                                hyperParameterTunning = classification['_source'][clk]
                            }
                            if (clk == 'bestModelHyperParams') {
                                bestModelHyperParams = classification['_source'][clk]
                                if (bestModelHyperParams && bestModelHyperParams.indexOf(',') > -1) {
                                    bestModelHyperParams = bestModelHyperParams.split(',');
                                    for (var i = 0; i < bestModelHyperParams.length; i++) {
                                        if (bestModelHyperParams[i].indexOf('::')) {
                                            var str = bestModelHyperParams[i].split('::');
                                            bestModelHyperParamsHeader.push(str[0]);
                                            bestModelHyperParamsValues.push(str[1]);
                                        }
                                    }
                                }
                            }
                            if (clk === 'bestModel') {
                                bestModel = classification['_source'][clk];
                            }
                            if (clk == 'featureImportance') {
                                featureImportance = classification['_source'][clk];
                            }
                        }
                        obj = {
                            branchPath: branchPath,
                            type: type,
                            content: [{
                                header: header,
                                row: row,
                                clusterIndex: clusterIndex,
                                confusionMetricData: confusionMetricDataMulti,
                                labelCorrespondMetric: value,
                                hyperParameterTunning: hyperParameterTunning,
                                bestModelHyperParamsHeader: bestModelHyperParamsHeader,
                                bestModelHyperParamsValues: bestModelHyperParamsValues,
                                decileDataRow: decileDataRow,
                                featureImportance: featureImportance,
                                bestModel: bestModel
                            }
                            ]
                        }
                        childType.push(obj);
                        header = [];
                        row = [];
                        confusionMetricDataMulti = [];
                        value = [];
                        labelCorrespondMetricHeader = [];
                        hyperParameterTunning = '';
                        bestModelHyperParams = '';
                        bestModelHyperParamsHeader = [];
                        bestModelHyperParamsValues = [];
                        decileDataHeader = [];
                        decileDataRow = [];
                        featureImportance = {};
                        bestModel = 'false';
                    })
                }
                if (key === 'RegressionType' || key == 'SurvivalAnalysisType') {
                    dat[key].forEach(function (regression) {
                        for (var reg in regression['_source']) {
                            if (reg != 'userId' &&
                                reg != 'pipelineId' &&
                                reg != 'branchId' &&
                                reg != 'algoName' &&
                                reg != 'clusterIndex' &&
                                reg != 'algoType' &&
                                reg != 'branchPath' &&
                                reg != 'hyperParameterTunning' &&
                                reg != 'bestModelHyperParams' &&
                                reg != 'coefficientHeaders' &&
                                reg != 'coefficientValues' &&
                                reg != 'DecileHeaders' &&
                                reg != 'ResponsePercentages' &&
                                reg != 'timeStampTypeD' &&
                                reg != 'projectId' &&
                                reg != 'algoinput' &&
                                reg != 'featureImportance' &&
                                reg != 'estype' &&
                                reg != 'indexOrder'
                            ) {
                                if (regression['_source'][reg] !== "NA" && regression['_source'][reg] !== "N/A") {
                                    header.push(reg);
                                    row.push(regression['_source'][reg]);
                                }
                            }
                            if (reg == 'branchPath') {
                                branchPath = regression['_source'][reg];
                                type = 'Regression'
                            }
                            if (reg == 'clusterIndex') {
                                clusterIndex = regression['_source'][reg]
                            }
                            if (reg == 'coefficientHeaders') {
                                coefficientHeaders = regression['_source'][reg].split(',');
                            }
                            if (reg == 'coefficientValues') {
                                coefficientValues = regression['_source'][reg].split(',');
                            }
                            if (reg == 'hyperParameterTunning') {
                                hyperParameterTunning = regression['_source'][reg]
                            }
                            if (reg == 'bestModelHyperParams') {
                                bestModelHyperParams = regression['_source'][reg]
                                if (bestModelHyperParams && bestModelHyperParams.indexOf(',') > -1) {
                                    bestModelHyperParams = bestModelHyperParams.split(',');
                                    for (var i = 0; i < bestModelHyperParams.length; i++) {
                                        if (bestModelHyperParams[i].indexOf('::')) {
                                            var str = bestModelHyperParams[i].split('::');
                                            bestModelHyperParamsHeader.push(str[0]);
                                            bestModelHyperParamsValues.push(str[1]);
                                        }
                                    }
                                }
                            }
                            if (reg === 'bestModel') {
                                bestModel = regression['_source'][reg];
                            }
                            if (reg === 'featureImportance') {
                                featureImportance = regression['_source'][reg];
                            }
                        }
                        obj = {
                            branchPath: branchPath,
                            type: type,
                            content: [{
                                header: header,
                                row: row,
                                clusterIndex: clusterIndex,
                                coefficientHeaders: coefficientHeaders,
                                coefficientValues: coefficientValues,
                                hyperParameterTunning: hyperParameterTunning,
                                bestModelHyperParamsHeader: bestModelHyperParamsHeader,
                                bestModelHyperParamsValues: bestModelHyperParamsValues,
                                decileDataRow: decileDataRow,
                                featureImportance: featureImportance,
                                bestModel: bestModel
                            }
                            ]
                        }
                        childType.push(obj);
                        header = [];
                        row = [];
                        coefficientHeaders = [];
                        coefficientValues = [];
                        hyperParameterTunning = '';
                        bestModelHyperParams = '';
                        bestModelHyperParamsHeader = [];
                        bestModelHyperParamsValues = [];
                        decileDataHeader = [];
                        decileDataRow = [];
                        featureImportance = {};
                        bestModel = 'false';
                    })
                }
                if (key === 'timeSeriesType') {
                    dat[key].forEach(function (timeSeries) {
                        for (var clKey in timeSeries['_source']) {
                            if (clKey == 'algoName') {
                                algoName = timeSeries['_source'][clKey]
                                type = "TimeSeries"
                            }
                            if (clKey == 'nextPredictionHeader') {
                                if (timeSeries['_source'][clKey].indexOf(',')) {
                                    nextPredictionHeader = timeSeries['_source'][clKey].split(',');
                                } else {
                                    nextPredictionHeader = [timeSeries['_source'][clKey]];
                                }
                                segmentFlag = false;
                            }
                            if (clKey == 'nextPredictionRow') {
                                if (timeSeries['_source'][clKey].indexOf(',')) {
                                    nextPredictionRow = timeSeries['_source'][clKey].split(',');
                                } else {
                                    nextPredictionRow = [timeSeries['_source'][clKey]];
                                }
                                segmentFlag = false
                            }
                            if (clKey === 'segmentData') {
                                segmentData = timeSeries['_source'][clKey];
                                segmentFlag = true;
                            }

                        }
                        obj = {
                            header: nextPredictionHeader,
                            row: nextPredictionRow,
                            algoName: algoName,
                            segmentFlag: segmentFlag,
                            segmentData: segmentData
                        }
                        timeSeriesType.push(obj);
                    })
                }
            }
        })
        /*var count;
        for(var i = 0; i < childType.length; i++){
            count = 0;
            for(var j = 0; j < childType.length; j++){
                if(childType[i].branchPath === childType[j].branchPath){
                    if(count == 0){
                        count++
                    }else{
                        childType[i].content.push(childType[j].content[0]);
                        childType.splice(j,1);
                    }
                }
            }
        }
        var retObj = {
            rootType:rootType,
            childType:childType
        }*/
        var uniqueArray = this.removeDuplicates(childType, "branchPath");
        var childData = [];
        for (var i = 0; i < uniqueArray.length; i++) {
            var content = [];
            for (var j = 0; j < childType.length; j++) {
                if (uniqueArray[i].branchPath == childType[j].branchPath) {
                    content.push(childType[j].content[0])
                }
            }
            var obj = {
                branchPath: uniqueArray[i].branchPath,
                type: uniqueArray[i].type,
                content: content
            }
            childData.push(obj);
        }
        var retObj = {
            rootType: rootType,
            childType: childData,
            timeSeriesType: timeSeriesType
        }
        return retObj;
    }

    removeDuplicates(originalArray, prop) {
        var newArray = [];
        var lookupObject = {};

        for (var i in originalArray) {
            lookupObject[originalArray[i][prop]] = originalArray[i];
        }

        for (i in lookupObject) {
            newArray.push(lookupObject[i]);
        }
        return newArray;
    }

    checkForHeaderAndRowChange(sourceData, header, row) {
        var newHeader = [], newRow = [], dataToChange;
        if (sourceData['algoName'] && sourceData['algoName'] == "LdaModel") {
            dataToChange = sourceData['ldaWords'];
            for (var i = 0; i < dataToChange.length; i++) {
                newHeader.push("topic_" + dataToChange[i].topic);
                newRow.push(dataToChange[i].wordsNew);
            }
            newHeader = ["timestamp", ...newHeader];
            newRow = [sourceData["timestamp"], ...newRow];
            return [newHeader, newRow];
        } else {
            return [header, row];
        }
    }

    async searchAutoDexPipeline(params) {
        let dexSearchJson = params.dexSearchJson;
        let indexAndType = params.indexAndType.split('/');
        let searchIndex = staticConf.autodexSearchPipeline.index;
        let searchType = staticConf.autodexSearchPipeline.type;
        let body = { "query": { "term": { "userQueryJson": dexSearchJson } } };
        //console.log("inde : ", searchIndex, "  type", searchType, "   body", body);
        let response = await esUtil.getEsData({ index: searchIndex, type: searchType }, body);
        if (response.error || !response.data.hits.hits.length) {
            let putRes = await this.putDataInIndex(indexAndType[0], indexAndType[1], dexSearchJson, response);
            return putRes;
        }
        response.saved = true;
        return response;
    }

    async putDataInIndex(index, type, json, response) {
        let obj = { index: staticConf.autodexSearchPipeline.index, type: staticConf.autodexSearchPipeline.type };
        if (response.error) {
            obj.body = staticConf.autodexSearchPipeline.mappingBody;
            let indicesCreateRes = await esUtil.createIndices({ index: staticConf.autodexSearchPipeline.index });
            let indicesMappingRes = await esUtil.putMapping(obj);
            delete obj.body;
        }
        obj.body = { dexindex: index, dextype: type, userQueryJson: json };
        let putEsDataRes = await esUtil.putEsData(obj)
        return putEsDataRes;
    }

    async explorationResult(params) {
        let resultSets = [];
        let body = this.getBody(params);
        let expResult = await esUtil.getEsData(params, body);
        if (expResult.error) {
            return expResult.errorMsg;
        }
        expResult = expResult.data.hits.hits;
        resultSets = this.modelPrepareForExp(expResult, resultSets);
        resultSets = this.getContentPrepareForExp(resultSets);
        return resultSets;
    }

    modelPrepareForExp(expResult, resultSets) {
        for (const iterator of expResult) {
            let obj = {
                timestamp: iterator._source.timestamp,
                ColNames: iterator._source.ColNames,
                segmentExists: iterator._source.noOfSegment ? true : false,
                segments: iterator._source.noOfSegment ? iterator._source.clusterIndex : 0,
                data: iterator._source.outPutJson
            };
            if (obj.ColNames) {
                resultSets.push(obj);
            }
        }
        return resultSets;
    }

    getContentPrepareForExp(response) {
        const _this = this;
        let result = {
            segmentExists: false,
            expResult: true,
            resultType: "",
            resultData: [],
            resultDataTypeTwo: []
        }
        if (response[0] && response[0].segmentInfo && response[0].segmentExists) {
            result.segmentExists = response[0].segmentExists
        }
        for (var i = 0; i < response.length; i++) {
            var tempData = [];
            var tempDataTypeTwo = [];
            var typeTwoFlag = false;
            for (var j = 0; j < 20; j++) {
                if (j === 0) {
                    tempData.push(response[i].ColNames);
                } else {
                    tempData.push('NA');
                }
                if (j === 19) tempData.push(response[i].segments);
            }

            for (var k = 0; k < 5; k++) {
                if (k === 0) {
                    tempDataTypeTwo.push(response[i].ColNames)
                } else if (k === 1) {
                    tempDataTypeTwo.push(response[i].segments)
                } else {
                    tempDataTypeTwo.push('NA');
                }
            }

            for (var key in response[i].data) {
                staticConf.typeOneData.forEach(function (itmes1, index1) {
                    for (var typeOneKey in itmes1) {
                        if (typeOneKey === key) {
                            for (var singleDimKey in response[i].data[key][0]) {
                                if (singleDimKey === 'DistinctValue') {
                                    var distinctValueData = _this.processDataForDistinctValue(response[i].data[key][0][singleDimKey]);
                                    tempData[itmes1[typeOneKey]] = distinctValueData;
                                } else {
                                    tempData[itmes1[typeOneKey]] = response[i].data[key][0][singleDimKey];
                                }
                            }
                        }
                    }
                })
                staticConf.typeTwoData.forEach(function (itmes2, index2) {
                    for (var typeTwoKey in itmes2) {
                        if (typeTwoKey === key) {
                            typeTwoFlag = true;
                            tempDataTypeTwo[itmes2[typeTwoKey]] = response[i].data[key][0].value;
                        }
                    }
                })
                staticConf.typeThreeData.forEach(function (itmes3, index3) {
                    for (var typeThreeKey in itmes3) {
                        if (typeThreeKey === key) {
                            var typeThreeValue = '';
                            for (var combKey in response[i].data[key][0]) {
                                if (!typeThreeValue) {
                                    typeThreeValue = combKey + ' : ' + response[i].data[key][0][combKey];
                                } else {
                                    typeThreeValue += '\r\n' + combKey + ' : ' + response[i].data[key][0][combKey];
                                }
                            }
                            tempData[itmes3[typeThreeKey]] = typeThreeValue;
                        }
                    }
                })
                staticConf.typeFourData.forEach(function (itmes4, index4) {
                    for (var typeFourKey in itmes4) {
                        if (typeFourKey === key) {
                            console.log('response[i].data == ' + JSON.stringify(response[i].data) + "\n");
                            if (key === 'BoxPlot' || key === 'TargetDistributionPlot') {    // Add here || for More Boxplot like chart
                                tempData[itmes4[typeFourKey]] = response[i].data[key];
                            } else {
                                tempData[itmes4[typeFourKey]] = response[i].data[key][0];
                            }
                        }
                    }
                })
            }
            if (response[i].ColNames.indexOf(',') === -1) {
                result.resultType = 'single';
                result.resultData.push(tempData);
            }
            if (typeTwoFlag) {
                result.resultType = 'multi';
                result.resultDataTypeTwo.push(tempDataTypeTwo);
            }
        }
        if (result.resultDataTypeTwo.length) {
            result = this.mergeReult(result);
        }
        return result;
    }

    mergeReult(result) {
        if (!result.segmentExists) {
            result.resultDataTypeTwo = this.mergeTwoDimDuplcateResult(result.resultDataTypeTwo);
        } else {
            result.resultDataTypeTwo = this.mergeTwoDimDuplcateResultForSegment(result.resultDataTypeTwo);
        }
        return result;
    }

    mergeTwoDimDuplcateResultForSegment(resultDataTypeTwo) {
        let headersForMulti = [];
        let noOfcolumns = resultDataTypeTwo[0].length;     // multi Dimension size
        let naArrayForMulti = [];
        headersForMulti = this.prepareHeaderForMultiType(headersForMulti, resultDataTypeTwo);
        for (let i = 0; i < headersForMulti.length; i++) {
            let arr = [];
            arr.push(headersForMulti[i].split('#')[0], headersForMulti[i].split('#')[1]);
            naArrayForMulti.push(arr);
        }
        naArrayForMulti = this.fillNAformulti(naArrayForMulti, noOfcolumns);
        for (let i = 0; i < naArrayForMulti.length; i++) {
            for (let j = 0; j < resultDataTypeTwo.length; j++) {
                if (naArrayForMulti[i][0] === resultDataTypeTwo[j][0] && naArrayForMulti[i][1] === resultDataTypeTwo[j][1]) {
                    for (let k = 2; k < naArrayForMulti[i].length; k++) {
                        if (resultDataTypeTwo[j][k] !== "NA") {
                            naArrayForMulti[i][k] = resultDataTypeTwo[j][k];
                        }
                    }
                }
            }
        }
        return naArrayForMulti;
    }

    prepareHeaderForMultiType(headersForMulti, resultDataTypeTwo) {      // prepare headers for multi case
        _.forEach(resultDataTypeTwo, function (o) {
            headersForMulti.push(o[0] + '#' + o[1]);
        })
        return _.uniq(headersForMulti);
    }

    fillNAformulti(naArrayForMulti, noOfcolumns) {
        for (let i = 0; i < naArrayForMulti.length; i++) {
            for (let j = 2; j < noOfcolumns; j++) {                 // 0 & 1 is reserved for header and segments
                naArrayForMulti[i].push("NA");
            }
        }
        return naArrayForMulti;
    }

    mergeTwoDimDuplcateResult(resultDataTypeTwo) {
        var firstIndexArr = [], secondArrOfNaExceptColName = [], filterTwoTypeArray = [], length = 0;
        _.forEach(resultDataTypeTwo, function (o) {
            firstIndexArr.push(o[0]);
            if (!length) length = o.length;
        })
        firstIndexArr = _.uniq(firstIndexArr);
        secondArrOfNaExceptColName.length = length - 1;
        secondArrOfNaExceptColName = _.fill(secondArrOfNaExceptColName, 'NA', 0, length - 1);
        filterTwoTypeArray = this.getFirstAndSecondArr(firstIndexArr, secondArrOfNaExceptColName, filterTwoTypeArray);
        filterTwoTypeArray = this.getFinalArr(filterTwoTypeArray, resultDataTypeTwo, length);
        return filterTwoTypeArray;
    }

    getFirstAndSecondArr(firstIndexArr, secondArrOfNaExceptColName, filterTwoTypeArray) {
        _.forEach(firstIndexArr, function (o) {
            var firstValArr = [];
            firstValArr.push(o);
            firstValArr = firstValArr.concat(secondArrOfNaExceptColName);
            filterTwoTypeArray.push(firstValArr);
        })
        return filterTwoTypeArray;
    }

    getFinalArr(filterTwoTypeArray, resultDataTypeTwo) {
        _.forEach(filterTwoTypeArray, function (e) {
            _.forEach(resultDataTypeTwo, function (m) {
                if (m[0] === e[0]) {
                    for (var i = 1; i < m.length; i++) {
                        if (m[i] !== 'NA') e[i] = m[i];
                    }
                }
            })
        })
        return filterTwoTypeArray;
    }

    processDataForDistinctValue(distinctValueData) {
        var distinctData;
        var bin = 5;
        var distinctValueResult = '';
        var keyValue, seprator, key, value;
        if (distinctValueData && distinctValueData.indexOf(',') > -1) {
            distinctData = distinctValueData.split(':');
            var thresholdValue = parseInt(distinctData[1]);
            if (thresholdValue > 50) {
                return thresholdValue;
            } else {
                distinctValueData = distinctData[2].split('#');
                for (var i = 0; i < distinctValueData.length; i++) {
                    keyValue = distinctValueData[i].split(',');
                    key = keyValue[0].substr(1);
                    value = keyValue[1].slice(0, -1);
                    seprator = i + 1;
                    if (seprator % bin === 0) {
                        distinctValueResult += ',' + key + ':' + value + '\r\n~';
                    } else {
                        if (!distinctValueResult) {
                            distinctValueResult = key + ':' + value;
                        } else {
                            var chekLastChar = distinctValueResult.slice(-1);
                            if (chekLastChar === '~') {
                                distinctValueResult = distinctValueResult.slice(0, -1);
                                distinctValueResult += key + ':' + value;
                            } else {
                                distinctValueResult += ',' + key + ':' + value;
                            }
                        }
                    }
                }
                if (distinctValueResult.slice(-1) === '~') distinctValueResult = distinctValueResult.slice(0, -1)
                return distinctValueResult;
            }
        } else {
            return distinctValueData;
        }
    }

    async viewApplicationLog(query) {
        let body = { "query": { "bool": { "must": [{ "match": { "applicationid": query.applicationId } }, { "match": { "timestamp": query.timestamp } }] } } }
        let logs = await esUtil.getEsData({ index: dynamicConf.viewLogIndex, type: '_doc' }, body);
        return logs;
    }

    async esDataByQuery(params) {
        let esData = await esUtil.getEsData({ index: params.index, type: params.type }, params.body);
        return esData;
    }
}
module.exports = new modelResultController();