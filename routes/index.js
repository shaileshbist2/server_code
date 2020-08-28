const express = require('express');
const router = express.Router();

router.use('/bucket',require('./bucket/index').router);
router.use('/auth/login', require('./auth/login').router);
router.use('/auth/register', require('./auth/register').router);
router.use('/pipeline',require('./data-api/pipeline/pipeline').router);
router.use('/prod',require('./prod-api/prod').router);

module.exports = router;
