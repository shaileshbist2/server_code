const express = require('express');
const passport = require('passport');
const router = express.Router();
const pipelineController = require('../../../controllers/pipelineController');
const modelResultController = require('../../../controllers/modelResultController')
const reqResUtility = require('../../../utils/res');
const appUtil = require('../../../utils/appUtil');
const { upload, gfs } = require('../../../controllers/grid-fs');

router.post('/posted', async (req, res) => {
    const params = req.body;
    try {
        if (params.pipelineSaveCase && params.pipelineAlgo === 'development') {
            params['modelAlgoParameters'] = await appUtil.makeParamsForRunPipeline(params, false);
        }
        const response = await pipelineController.saveOrUpdate(params);
        if (response === 'project_exists') {
            return reqResUtility.createResponse(req, res, false, params.projectName + ' already exists!', {});
        } else if (response === 'pipeline_exists') {
            return reqResUtility.createResponse(req, res, false, params.algoName + ' already exists!', {});
        } else {
            reqResUtility.createResponse(req, res, false, 'success', { _id: response._id });
        }
    } catch (err) {
        if (err) {
            console.log("Error Log : ", err);
        }
        return reqResUtility.createResponse(req, res, true, 'fail', err);
    }
});

router.post('/runPipeline', passport.authenticate('jwt', { session: false }), (req, res) => {
    const params = req.body;
    params['username'] = req.user.username;
    pipelineController.runPipeline(params);
    //no need to wait for send response till pipeline complete as it will handle by socket
    reqResUtility.createResponse(req, res, false, 'success', {});
});

router.post('/list', async (req, res) => {
    const data = await pipelineController.list(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.get('/readHdfs', async (req, res) => {
    const request = req.query;
    try {
        const response = await pipelineController.readHdfsData(request);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        console.log(error);
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
});

router.post('/modelResult', async (req, res) => {
    const request = req.body;
    try {
        const response = await modelResultController.getModelData(request);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/searchDexEs', async (req, res) => {
    let body = req.body;
    try {
        const response = await modelResultController.searchAutoDexPipeline(body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        console.log("Error :", error);
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
});

router.get('/get', async (req, res) => {
    let request = req.query;
    let options = appUtil.makeOptions('GET', request.reqUrl, JSON.stringify({}), false, request.timeout, { "content-type": "application/json" });
    try {
        let response = await appUtil.apiCall(options, false);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/post', async (req, res) => {
    let body = req.body;
    let url = body.reqUrl;
    delete body.reqUrl;
    let options = appUtil.makeOptions('POST', url, body, true, body.timeout, { "content-type": "application/json" });
    try {
        let response = await appUtil.apiCall(options, false);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.get('/getPipelineById', async (req, res) => {
    try {
        const response = await pipelineController.getPipelineById(req.query);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.get('/getPipelineByKey', async (req, res) => {
    try {
        const response = await pipelineController.getPipelineByKey(req.query);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/explorationResult', async (req, res) => {
    const body = req.body;
    try {
        const response = await modelResultController.explorationResult(body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        //console.log("errrrrrrrrrr ", error);
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/getCartesianProduct', async (req, res) => {
    const body = req.body, resArr = [];
    try {
        let argue = body.arrays;
        let cProduct = await appUtil.cartesianProductOf(...argue);
        for (var index in cProduct) {
            resArr.push(cProduct[index].join('_'));
        }
        reqResUtility.createResponse(req, res, false, 'success', resArr)
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.get('/viewApplicationLog', async (req, res) => {
    const query = req.query;
    try {
        const response = await modelResultController.viewApplicationLog(query);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
});

router.get('/killApplicationlivy', async (req, res) => {
    const query = req.query;
    try {
        let response = await appUtil.killApplicationlivy(query);
        const request = { tableName: 'process', processStatus: 'Killed', _id: query._id };
        response = pipelineController.saveOrUpdate(request);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        console.log("deeeeeeeeeeeeeeeeeeeeee ", error);
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/deletePipeline', async (req, res) => {
    try {
        const response = await pipelineController.deletePipeline(req.body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
        console.log(error);
    }
})

router.post('/deleteProject', async (req, res) => {
    try {
        const response = await pipelineController.deleteProject(req.body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
        console.log(error);
    }
})

router.post('/esDataByQuery', async (req, res) => {
    try {
        const response = await modelResultController.esDataByQuery(req.body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
        console.log(error);
    }
})

router.get('/readHive', async (req, res) => {
    try {
        const response = await pipelineController.readHive(req.query);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        console.log(error);
        reqResUtility.createResponse(req, res, true, 'fail', error);
    }
})

router.post('/paginate_list', async (req, res) => {
    const data = await pipelineController.paginate_list(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.post('/createFile', async (req, res) => {
    try {
        const response = await pipelineController.createFile(req.body);
        reqResUtility.createResponse(req, res, false, 'success', response);
    } catch (error) {
        reqResUtility.createResponse(req, res, true, 'fail', error);
        console.log(error);
    }
})
module.exports.router = router;
