const cliMsg = require('./cli-msg/api-msg');
const config = require('../config/serverDynamicConfig.json');

function createResponse(req,
    res,
    error = false,
    message,
    response = {}) {
    let output = {
        api: req.originalUrl,
        status: error ? 400 : 200,
        error: error,
        message: message,
        response: response
    };
    if (config.handleControl.apiErrorMsg && process.env.NODE_ENV === 'dev') cliMsg.apiMessage(output, req.originalUrl);
    if (error) {
        res.status(output.status).send(output);
    } else {
        res.send(output);
    }
}

module.exports = {
    createResponse
};
