const express = require('express');
const router = express.Router();
const prodController = require('../../controllers/prodController');
const reqResUtility = require('../../utils/res');

router.post('/isAlgoIdExists_in_pipeline_prod', async (req, res) => {
    const data = await prodController.isAlgoIdExists_in_pipeline_prod(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.post('/checkStatus', async (req, res) => {
    const data = await prodController.checkStatus(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.post('/is_project_name_exist', async (req, res) => {
    const data = await prodController.is_project_name_exist(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.post('/project', async (req, res) => {
    const data = await prodController.project(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

router.post('/pipeline', async (req, res) => {
    const data = await prodController.pipeline(req.body);
    reqResUtility.createResponse(req, res, false, 'success', data);
});

module.exports.router = router;
